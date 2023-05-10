import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path

from raft.tests.common_tcase import TestCaseCommon

from raft.dev_tools.pausing_app import InterceptorMode
from raft.messages.heartbeat import HeartbeatMessage
from raft.messages.heartbeat import HeartbeatResponseMessage
from raft.messages.append_entries import AppendEntriesMessage
from raft.messages.append_entries import AppendResponseMessage
from raft.messages.request_vote import RequestVoteMessage
from raft.messages.request_vote import RequestVoteResponseMessage
from raft.messages.status import StatusQueryResponseMessage
from raft.states.base_state import Substate, StateCode
from raft.dev_tools.bt_client import MemoryBankTellerClient
from raft.dev_tools.ps_cluster import PausingServerCluster
from raft.dev_tools.pausing_app import PausingMonitor, PFollower, PLeader
from raft.dev_tools.pausing_app import PCandidate
from raft.dev_tools.timer_wrapper import get_all_timer_sets

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.1

class ModLeader(PLeader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        self.logger.info("\n\n\t ModLeader started \n\n")
        super().start()

        
class ModFollower(PFollower):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        self.logger.info("\n\n\t ModFollower started \n\n")
        super().start()

class ModCandidate(PCandidate):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pause_before_vote_start = False
        self.paused = False

    def start(self):
        self.logger.info("\n\n\t ModCandidate started \n\n")
        super().start()

    async def start_election(self):
        if self.pause_before_vote_start:
            self.logger.info("\n\n\t ModCandidate pausing in start_election \n\n")
            self.paused = True
        await super().start_election()


class ModMonitor(PausingMonitor):

    def __init__(self, orig_monitor):
        super().__init__(orig_monitor.pbt_server,
                         orig_monitor.name,
                         orig_monitor.logger)
        self.state_map = orig_monitor.state_map
        self.state = orig_monitor.state
        self.substate = orig_monitor.substate
        self.pbt_server.state_map.remove_state_change_monitor(orig_monitor)
        self.pbt_server.state_map.add_state_change_monitor(self)
        self.leader = None
        self.follower = None
        self.candidate = None
        self.pause_before_vote_start = False
        
    async def new_state(self, state_map, old_state, new_state):
        if new_state.code == StateCode.leader:
            new_state = ModLeader(new_state.server,
                                  new_state.heartbeat_timeout)
            self.leader = new_state
            return new_state
        if new_state.code == StateCode.follower:
            new_state = ModFollower(new_state.server,
                                    new_state.timeout)
            self.follower = new_state
            return new_state
        if new_state.code == StateCode.candidate:
            new_state = ModCandidate(new_state.server,
                                     new_state.timeout)
            self.candidate = new_state
            new_state.pause_before_vote_start = self.pause_before_vote_start 
            return new_state
        new_state = await super().new_state(state_map, old_state, new_state)
        return new_state
    
class TestCandidateVoteStartPause(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingServerCluster(server_count=3,
                                            logging_type=LOGGING_TYPE,
                                            base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.logger = logging.getLogger("raft.tests." + __name__)
                
    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.1)
        self.loop.close()

    def preamble(self, slow=False):
        if slow:
            tb = 1.0
        else:
            tb = timeout_basis
        servers = self.cluster.prepare(timeout_basis=tb)
        # start just the one server and wait for it
        # to pause in candidate state
        first_mon = None
        for sname in ("server_0", "server_1", "server_2"):
            spec = servers[sname]
            monitor = spec.monitor
            spec.monitor = monitor = ModMonitor(monitor)
            spec.pbt_server.replace_monitor(monitor)
            monitor.set_pause_on_substate(Substate.voting)
            if first_mon is None:
                first_mon = monitor
        self.cluster.start_one_server("server_0")
        self.logger.info("waiting for switch to candidate")
        start_time = time.time()
        while time.time() - start_time < 10 * timeout_basis:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if first_mon.state is not None:
                if str(first_mon.state) == "candidate":
                    if first_mon.substate == Substate.voting:
                        if first_mon.pbt_server.paused:
                            break
        
        self.assertEqual(first_mon.substate, Substate.voting)
        self.assertTrue(first_mon.pbt_server.paused)
        return spec
        
    def test_1(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        self.logger.info("Candidate paused")
        monitor.clear_substate_pauses()
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        
class TestElectionStartPaused(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingServerCluster(server_count=3,
                                            logging_type=LOGGING_TYPE,
                                            base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.logger = logging.getLogger("raft.tests." + __name__)
                
    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.1)
        self.loop.close()

    def preamble(self, slow=False):
        if slow:
            tb = 1.0
        else:
            tb = timeout_basis
        self.timeout_basis = tb
        def cb():
            for spec in self.servers.values():
                monitor = spec.monitor
                spec.monitor = monitor = ModMonitor(monitor)
                spec.pbt_server.replace_monitor(monitor)
            
        super().preamble(num_to_start=3, pre_start_callback=cb)
        
    def test_base_flow(self):
        """ An example of how to get to the point just prior to election
        after leader dies. Flesh this out for actual tests"""
        
        self.preamble()
        self.logger.info("Started")
        for spec in self.servers.values():
            spec.monitor.set_pause_on_substate(Substate.leader_lost)
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        old_leader = self.leader
        self.cluster.stop_server(old_leader.name)
        start_time = time.time()
        while time.time() - start_time < timeout_basis * 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in self.servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
            if pause_count == 2:
                break

        # Now we have two followers that have detected leader lost,
        # so without intervention they will go to candidate mode next.
        # For and actual test, you want to fiddle the state of things
        # to ensure that you have your test conditions before you restart
        # the servers.
        for spec in self.servers.values():
            spec.monitor.clear_substate_pauses()
        self.cluster.resume_all_paused_servers()
        

    def test_state_split(self):
        """ An example of how to get to the point just prior to election
        after leader dies with one server already switched to Candidate
        and the other still a follower. Flesh this out for actual tests"""
        
        self.preamble()
        self.logger.info("Started")
        candi = self.non_leaders[0]
        follower = self.non_leaders[1]
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()

        # pause the expected follower at the next heartbeat
        follower.interceptor.add_trigger(InterceptorMode.in_before, 
                                         HeartbeatMessage._code,
                                         self.pausers[follower.name].follower_pause)
        self.logger.info(f"\n\nAllowing follower {follower.name} pause on heartbeat\n\n")
        time.sleep(0.05)
        # pause the expected candidate at the leader lost 
        candi.monitor.set_pause_on_substate(Substate.leader_lost)
        old_leader = self.leader
        self.cluster.stop_server(old_leader.name)
        start_time = time.time()
        paused_list = []
        while time.time() - start_time < timeout_basis * 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            paused_list = []
            for spec in self.servers.values():
                if spec.pbt_server.paused:
                    paused_list.append(spec)
            if len(paused_list) == 2:
                break
        self.logger.info(f"\n\nPaused with {candi.name} about to switch to candidate"
                         "and {follower.name} still following\n\n")
        self.clear_intercepts()
        candi.monitor.clear_substate_pauses()
        self.logger.info("\n\n\n Resuming servers \n\n\n")
        self.cluster.resume_all_paused_servers()
        

        
