import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path

from tests.common_tcase import TestCaseCommon

from dev_tools.pausing_app import InterceptorMode
from raftframe.messages.heartbeat import HeartbeatMessage
from raftframe.messages.heartbeat import HeartbeatResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage
from raftframe.messages.append_entries import AppendResponseMessage
from raftframe.messages.request_vote import RequestVoteMessage
from raftframe.messages.request_vote import RequestVoteResponseMessage
from raftframe.messages.status import StatusQueryResponseMessage
from raftframe.states.base_state import Substate, StateCode
from dev_tools.bt_client import MemoryBankTellerClient
from dev_tools.pcluster import PausingCluster

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.1


class TestCandidateVoteStartPause(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingCluster(server_count=3,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=timeout_basis)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.logger = logging.getLogger("tests." + __name__)
                
    def tearDown(self):
        self.cluster.stop_all()
        time.sleep(0.1)
        self.loop.close()

    def preamble(self):
        # start just the one server and wait for it
        # to pause in candidate state
        first_mon = None
        for server in self.cluster.servers:
            server.pause_on_substate(Substate.voting)
        server = self.cluster.servers[0]
        server.start()
        self.logger.info("waiting for switch to candidate")
        start_time = time.time()
        while time.time() - start_time < 10 * timeout_basis:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            state = server.state_map.get_state()
            if state is not None:
                if str(state) == "candidate":
                    if state.substate == Substate.voting:
                        if server.paused:
                            break
        
        self.assertEqual(state.substate, Substate.voting)
        self.assertTrue(server.paused)
        return server
        
    def test_1(self):
        pserver = self.preamble()
        candidate = pserver.state_map.get_state()
        self.logger.info("Candidate paused")
        pserver.clear_substate_pauses()
        pserver.resume()
        
class TestElectionStartPaused(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingCluster(server_count=3,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=timeout_basis)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.logger = logging.getLogger("tests." + __name__)
                
    def tearDown(self):
        self.cluster.stop_all()
        time.sleep(0.1)
        self.loop.close()

    def test_base_flow(self):
        """ An example of how to get to the point just prior to election
        after leader dies. Flesh this out for actual tests"""
        
        self.preamble()
        self.logger.info("Started")
        for pserver in self.cluster.servers:
            pserver.pause_on_substate(Substate.leader_lost)
        self.clear_intercepts()
        self.cluster.resume_all()
        old_leader = self.leader
        old_leader.stop()
        start_time = time.time()
        while time.time() - start_time < timeout_basis * 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for server in self.cluster.servers:
                if server.paused:
                    pause_count += 1
            if pause_count == 2:
                break

        # Now we have two followers that have detected leader lost,
        # so without intervention they will go to candidate mode next.
        # For and actual test, you want to fiddle the state of things
        # to ensure that you have your test conditions before you restart
        # the servers.
        for server in self.cluster.servers:
            server.clear_substate_pauses()
        self.cluster.resume_all()
        

    def test_state_split(self):
        """ An example of how to get to the point just prior to election
        after leader dies with one server already switched to Candidate
        and the other still a follower. Flesh this out for actual tests"""

        self.preamble()
        self.logger.info("Started")
        candi = self.non_leaders[0]
        follower = self.non_leaders[1]
        self.clear_intercepts()
        # pause the expected follower at the next heartbeat
        follower.pause_before_in_message(HeartbeatMessage._code)
        self.logger.info(f"\n\nSetting follower {follower.name} pause on heartbeat\n\n")
        self.cluster.resume_all()
        start_time = time.time()
        while time.time() - start_time < timeout_basis * 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if follower.paused:
                break
        self.assertTrue(follower.paused)
        # pause the expected candidate at the leader lost, which is before the
        # switch to candidate
        candi.pause_on_substate(Substate.leader_lost)
        # now stop the leader
        old_leader = self.leader
        old_leader.stop()
        start_time = time.time()
        while time.time() - start_time < timeout_basis * 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if candi.paused:
                break
        self.assertTrue(candi.paused)
        self.logger.info(f"\n\nPaused with {candi.name} about to switch to candidate"
                         "and {follower.name} still following\n\n")
        self.clear_intercepts()
        candi.clear_substate_pauses()
        follower.clear_message_triggers()
        self.logger.info("\n\n\n Resuming servers \n\n\n")
        self.cluster.resume_all()
        time.sleep(0.1)
        

        
