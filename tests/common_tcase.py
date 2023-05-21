import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raftframe.messages.heartbeat import HeartbeatMessage
from raftframe.messages.heartbeat import HeartbeatResponseMessage
from raftframe.messages.append_entries import AppendResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage
from raftframe.states.base_state import StateCode, Substate

from dev_tools.pcluster import PausingCluster

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

class TestCaseCommon(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.total_nodes = 3
        cls.timeout_basis = 0.1
        cls.logger_name = __name__
        
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingCluster(server_count=self.total_nodes,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=self.timeout_basis)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.logger = logging.getLogger(self.logger_name)
                
    def tearDown(self):
        self.cluster.stop_all()
        time.sleep(0.1)
        self.loop.close()

    def pause_waiter(self, label, expected=None, timeout=2):
        if expected is None:
            expected = self.total_nodes
        self.logger.info("waiting for %s", label)
        self.leader = None
        self.non_leaders = []
        start_time = time.time()
        while time.time() - start_time < timeout:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            self.non_leaders = []
            for server in self.cluster.servers:
                if server.paused:
                    pause_count += 1
                    if server.state_map.get_state().get_code() == StateCode.leader:
                        self.leader = server
                    else:
                        self.non_leaders.append(server)
            if pause_count >= expected:
                break
        self.assertEqual(pause_count, expected)
        self.assertIsNotNone(self.leader)
        self.assertEqual(len(self.non_leaders) + 1, expected)
        return

    def dump_state(self):
        for server in self.cluster.servers:
            server.dump_state()
            time.sleep(0.01)

    def resume_waiter(self):
        start_time = time.time()
        while time.time() - start_time < 5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for server in self.cluster.servers:
                if server.running:
                    if server.paused:
                        pause_count += 1
            if pause_count == 0:
                break
        self.assertEqual(pause_count, 0)

    def reset_roles(self):
        self.leader = None
        self.non_leaders = []
        
    def preamble(self, num_to_start=None,  pre_start_callback=None):
        if num_to_start is None:
            num_to_start = len(self.cluster.servers)
        self.pausers = {}
        self.leader = None
        self.non_leaders = []
        self.expected_followers = num_to_start - 1
        if pre_start_callback:
            pre_start_callback()
        started_count = 0
        for server in self.cluster.servers:
            # pause leader after new term record
            server.pause_on_substate(Substate.became_leader)
            # pause followers after they accept leader
            server.pause_on_substate(Substate.joined)
            
        for server in self.cluster.servers:
            server.start()
            started_count += 1
            if started_count == num_to_start:
                break

        self.pause_waiter("waiting for pause first election done (append)",
                          expected = started_count)

    def set_term_start_intercept(self, clear=True):
        """broken"""
        for server in self.servers:
            if clear:
                spec.interceptor.clear_message_triggers()
            pauser = self.pausers[spec.name]
            pauser.term_start_only = True
            spec.interceptor.add_trigger(InterceptorMode.out_after, 
                                         AppendEntriesMessage._code,
                                         pauser.leader_pause)
            spec.interceptor.add_trigger(InterceptorMode.in_before, 
                                         AppendEntriesMessage._code,
                                         pauser.follower_pause)

    def set_hb_intercept(self, clear=True):

        for spec in self.servers.values():
            if clear:
                spec.interceptor.clear_triggers()
            spec.interceptor.add_trigger(InterceptorMode.out_after, 
                                         HeartbeatMessage._code,
                                         self.pausers[spec.name].leader_pause)
            spec.interceptor.add_trigger(InterceptorMode.in_before, 
                                         HeartbeatMessage._code,
                                         self.pausers[spec.name].follower_pause)
    def clear_intercepts(self):
        for server in self.cluster.servers:
            server.clear_message_triggers()
        
    def postamble(self):
    
        for server in self.cluster.servers:
            server.clear_message_triggers()

        self.cluster.resume_all()
        
    def test_check_setup(self):
        # rename this to remove the a_ in order to
        # check the basic control flow if you think
        # it might be brokens
        self.preamble(num_to_start=3)
        self.clear_intercepts()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        self.postamble()

