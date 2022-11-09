import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.heartbeat import HeartbeatMessage
from raft.messages.heartbeat import HeartbeatResponseMessage
from raft.messages.command import ClientCommandMessage
from raft.states.base_state import StateCode
from raft.dev_tools.ps_cluster import PausingServerCluster
from raft.dev_tools.pausing_app import InterceptorMode, TriggerType
from raft.dev_tools.pauser import Pauser

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.1
        

class TestSplit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingServerCluster(server_count=5,
                                            logging_type=LOGGING_TYPE,
                                            base_port=5000,
                                            timeout_basis=timeout_basis)
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

    def pause_waiter(self, label, expected=5, timeout=2):
        self.logger.info("waiting for %s", label)
        self.leader = None
        start_time = time.time()
        while time.time() - start_time < timeout:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            self.non_leaders = []
            self.followers = []
            self.candidates = []
            for spec in self.servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
                    if spec.monitor.state.get_code() == StateCode.leader:
                        self.leader = spec
                    elif spec.monitor.state.get_code() == StateCode.follower:
                       self.followers.append(spec)
                       self.non_leaders.append(spec)
                    elif spec.monitor.state.get_code() == StateCode.candidate:
                       self.candidates.append(spec)
                       self.non_leaders.append(spec)
            if pause_count >= expected:
                break
        self.assertIsNotNone(self.leader)
        self.assertEqual(len(self.followers) + len(self.candidates) + 1,
                         expected)
        return 

    def resume_waiter(self):
        start_time = time.time()
        while time.time() - start_time < 5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in self.servers.values():
                if spec.running:
                    if spec.pbt_server.paused:
                        pause_count += 1
            if pause_count == 0:
                break
        self.assertEqual(pause_count, 0)

    def reset_pausers(self):
        for name in self.servers.keys():
            self.pausers[name].reset()

    def reset_roles(self):
        self.leader = None
        self.followers = []
        
    def preamble(self):
        self.servers = self.cluster.prepare()
        self.pausers = {}
        self.leader = None
        self.followers = []
        self.expected_followers = len(self.servers) - 1
        for spec in self.servers.values():
            self.pausers[spec.name] = Pauser(spec, self)
        self.set_hb_intercept()
        self.cluster.start_all_servers()
        self.pause_waiter("waiting for pause first election done (heartbeat)")

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
        for spec in self.servers.values():
            spec.interceptor.clear_triggers()
        
    def postamble(self):
    
        for spec in self.servers.values():
            spec.interceptor.clear_triggers()

        self.cluster.resume_all_paused_servers()
        
    def pause_and_break(self, go_after=True):
        # call this to get a breakpoint that has
        # everyone stopped
        time.sleep(1)
        self.reset_pausers()
        self.set_hb_intercept()
        breakpoint()
        if go_after:
            self.clear_intercepts()
            self.cluster.resume_all_paused_servers()

    def go_after_break(self):
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        
    def test_leader_stays_split(self):
        self.preamble()
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.resume_waiter()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        log_record_count = 1
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count += 1
        self.assertEqual(res['balance'], 10)
        time.sleep(0.01) # allow time for the commit messages to flow
        

        leader = self.leader
        lost_1 = self.non_leaders[0]
        lost_2 = self.non_leaders[1]
        kept_1 = self.non_leaders[2]
        kept_2 = self.non_leaders[3]

        self.logger.debug("\n\n\tIsolating two servers as though" \
                          " network partitioned, %s and %s\n\n",
                          lost_1.name, lost_2.name)

        lost_1.pbt_server.comms.set_partition(1)
        lost_2.pbt_server.comms.set_partition(1)
        
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()

        self.logger.debug("\n\n\tAdding 10 via do_credit\n\n")
        client.do_credit(10)
        log_record_count += 1
        start_time = time.time()
        while time.time() - start_time < 4:
            done = []
            for spec in [leader, kept_1, kept_2]:
                tlog = spec.server_obj.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(spec)
            
            if len(done) == 3:
                break
        self.assertEqual(len(done), 3, msg="Log commits missing for 4 seconds")
        for spec in [lost_1, lost_2]:
            tlog = spec.server_obj.get_log()
            # log indices are 1 offset, not zero
            self.assertTrue(tlog.get_commit_index() < log_record_count)
        self.logger.debug("\n\n\tHealing fake network partition\n\n")
        lost_1.pbt_server.comms.set_partition(0)
        lost_2.pbt_server.comms.set_partition(0)
        
        start_time = time.time()
        while time.time() - start_time < 4:
            done = []
            for spec in [lost_1, lost_2]:
                tlog = spec.server_obj.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(spec)
            
            if len(done) == 2:
                break
        self.assertEqual(len(done), 2,
                         msg="Post heal log commits missing for 4 seconds")
        
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
    def test_leader_lost_split(self):
        self.preamble()
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.resume_waiter()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        log_record_count = 1
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count += 1
        self.assertEqual(res['balance'], 10)
        time.sleep(0.01) # allow time for the commit messages to flow

        start_time = time.time()
        while time.time() - start_time < 2:
            done = []
            for spec in self.cluster.get_servers().values():
                tlog = spec.server_obj.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(spec)
            if len(done) == 5:
                break
            time.sleep(0.1)
        self.assertEqual(len(done), 5,
                         msg="Post heal log commits missing for 2 seconds")

        self.reset_pausers()
        self.set_hb_intercept()
        self.pause_waiter("Waiting for all to pause on heartbeat")
        leader = self.leader
        lost_1 = self.non_leaders[0]
        kept_1 = self.non_leaders[1]
        kept_2 = self.non_leaders[2]
        kept_3 = self.non_leaders[3]

        self.logger.debug("\n\n\tIsolating two servers as though" \
                          " network partitioned, leader %s and %s\n\n",
                          leader.name, lost_1.name)

        leader.pbt_server.comms.set_partition(1)
        lost_1.pbt_server.comms.set_partition(1)

        if False:
            # if you are debugging, you might want to do this
            # let the three kept servers run, but not the
            # "partitioned" ones
            for spec in self.servers.values():
                if spec in [kept_1, kept_2, kept_3]:
                    spec.interceptor.clear_triggers()
                    self.cluster.resume_paused_server(spec.name, wait=False)
        else:
            self.reset_pausers()
            self.clear_intercepts()
            self.cluster.resume_all_paused_servers()
        new_leader = None
        start_time = time.time()
        while time.time() - start_time < 2:
            for spec in [kept_1, kept_2, kept_3]:
                if str(spec.server_obj.state_map.state) == "leader":
                    new_leader = spec
                    break
            time.sleep(0.1)
        self.assertIsNotNone(new_leader)
        
        self.logger.debug("\n\n\tGot New leader %s," \
                          " Adding 10 via do_credit\n\n",
                          new_leader.name)
        client2 = new_leader.get_client()
        client2.do_credit(10)
        log_record_count += 1
        start_time = time.time()
        while time.time() - start_time < 2:
            done = []
            for spec in [kept_1, kept_2, kept_3]:
                tlog = spec.server_obj.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(spec)
            if len(done) == 3:
                break
            time.sleep(0.1)
        self.assertEqual(len(done), 3, msg="Log commits missing for 4 seconds")
        
        for spec in [leader, lost_1]:
            tlog = spec.server_obj.get_log()
            # log indices are 1 offset, not zero
            self.assertTrue(tlog.get_commit_index() < log_record_count)
        self.logger.debug("\n\n\tHealing fake network partition\n\n")
        leader.pbt_server.comms.set_partition(0)
        lost_1.pbt_server.comms.set_partition(0)
        for spec in self.servers.values():
            if spec in [leader, lost_1]:
                spec.interceptor.clear_triggers()
                self.cluster.resume_paused_server(spec.name, wait=False)
        
        start_time = time.time()
        while time.time() - start_time < 2:
            done = []
            for spec in [leader, lost_1]:
                tlog = spec.server_obj.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(spec)
            
            if len(done) == 2:
                break
            time.sleep(0.1)
        self.assertEqual(len(done), 2,
                         msg="Post heal log commits missing for 4 seconds")
        
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
