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

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.5
        
class Pauser:
    
    def __init__(self, spec, tcase):
        self.spec = spec
        self.tcase = tcase
        self.am_leader = False
        self.paused = False
        self.broadcasting = False
        self.sent_count = 0
        
    def reset(self):
        self.am_leader = False
        self.paused = False
        self.broadcasting = False
        self.sent_count = 0
        
    async def leader_pause(self, mode, code, message):
        self.am_leader = True
        self.tcase.leader = self.spec
        
        if self.broadcasting or code in ("term_start", "heartbeat"):
            # we are sending one to each follower, whether
            # it is running or not
            limit = len(self.tcase.servers) - 1
        else:
            limit = self.tcase.expected_followers
        self.sent_count += 1
        if self.sent_count < limit:
            self.tcase.logger.debug("not pausing on %s, sent count" \
                                    " %d not yet %d", code,
                                    self.sent_count, limit)
            return True
        self.tcase.logger.info("got sent for %s followers, pausing",
                               limit)
        await self.spec.pbt_server.pause_all(TriggerType.interceptor,
                                             dict(mode=mode,
                                                  code=code))
        return True
    
    async def follower_pause(self, mode, code, message):
        self.am_leader = False
        self.tcase.followers.append(self.spec)
        self.paused = True
        await self.spec.pbt_server.pause_all(TriggerType.interceptor,
                                             dict(mode=mode,
                                                  code=code))
        return True


class TestLogOps(unittest.TestCase):

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

    def pause_waiter(self, label, expected=3, timeout=2):
        self.logger.info("waiting for %s", label)
        self.leader = None
        start_time = time.time()
        while time.time() - start_time < timeout:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            self.followers = []
            for spec in self.servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
                    if spec.monitor.state.get_code() == StateCode.leader:
                        self.leader = spec
                    elif spec.monitor.state.get_code() == StateCode.follower:
                       self.followers.append(spec)
            if pause_count >= expected:
                break
        self.assertIsNotNone(self.leader)
        self.assertEqual(len(self.followers) + 1, expected)
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
        
    def a_test_check_setup(self):
        # rename this to remove the a_ in order to
        # check the basic control flow
        self.preamble()
        self.postamble()

    def test_restarts(self):
        self.preamble()
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
        

        # clear the role references
        orig_leader = self.leader
        self.leader = None
        self.logger.debug("\n\n\tStoping Leader %s \n\n", orig_leader.name)
        self.cluster.stop_server(orig_leader.name)
        self.expected_followers = 1
        self.reset_pausers()
        # just make sure election happens so we know who's who
        self.pause_waiter("waiting for pause leader lost new election",
                          expected=2)
        second_leader = self.leader
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tQuery to %s \n\n", second_leader.name)
        client2 = self.leader.get_client()
        res2 = client2.do_query()
        log_record_count += 1
        self.assertEqual(res2['balance'], 10)

        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        time.sleep(0.01) # allow time for the commit messages to flow
        
        self.logger.debug("\n\n\tRestarting original leader %s \n\n",
                          orig_leader.name)
        tname = orig_leader.name
        
        restarted = self.cluster.prepare_one(tname)
        self.pausers[tname] = Pauser(restarted, self)
        self.cluster.start_one_server(tname)
        self.logger.debug("\n\n\tActivating heartbeat intercepts \n\n")
        # wait for restarted server to catch up
        start_time = time.time()
        trec = None
        while time.time() - start_time < 4:
            tserver = restarted.server_obj
            if tserver:
                tlog = tserver.get_log()
                trec = tlog.read()
                if trec:
                    if trec.index == log_record_count -1:
                        break
            time.sleep(0.01)
        self.assertIsNotNone(trec)
        self.assertEqual(trec.index, log_record_count-1)
        self.logger.debug("\n\n\tRestarted %s log catch up done \n\n",
                          restarted.name)
        # kill and restart a follower and wait for it to have an up to date log
        target = self.followers[0]
        
        self.leader = None
        self.logger.debug("\n\n\tStopping follower %s \n\n", target.name)
        self.cluster.stop_server(target.name)
        self.logger.debug("\n\n\tRestarting follower %s \n\n", target.name)
        new_target = self.cluster.prepare_one(target.name)
        self.cluster.start_one_server(new_target.name)
        if False:
            time.sleep(1)
            self.reset_pausers()
            self.set_hb_intercept()
            breakpoint()
            self.clear_intercepts()
            self.cluster.resume_all_paused_servers()
            
        start_time = time.time()
        trec = None
        while time.time() - start_time < 4:
            tserver = new_target.server_obj
            if tserver:
                tlog = tserver.get_log()
                trec = tlog.read()
                if trec:
                    if trec.index == log_record_count -1:
                        break
            time.sleep(0.01)
        self.assertIsNotNone(trec)
        self.assertEqual(trec.index, log_record_count-1)
        self.logger.debug("\n\n\tRestarted follower %s log catch up done \n\n",
                          new_target.name)

        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
