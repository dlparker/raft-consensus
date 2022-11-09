import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.heartbeat import HeartbeatMessage
from raft.messages.heartbeat import HeartbeatResponseMessage
from raft.messages.append_entries import AppendResponseMessage
from raft.messages.command import ClientCommandMessage
from raft.states.base_state import StateCode
from raft.dev_tools.ps_cluster import PausingServerCluster
from raft.dev_tools.pausing_app import InterceptorMode, TriggerType
from raft.dev_tools.pausing_app import PausingMonitor, PLeader, PFollower
from raft.dev_tools.pauser import Pauser

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.1

class ModFollower(PFollower):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.have_append = False
        self.skip_to_append = False
        self.send_higher_term_on_hb = False
        self.send_higher_term_on_ae = False

    def set_skip_to_append(self, flag):
        self.skip_to_append = flag
    
    def set_send_higher_term_on_hb(self, flag):
        self.send_higher_term_on_hb = flag

    def set_send_higher_term_on_ae(self, flag):
        self.send_higher_term_on_ae = flag
        
    def start(self):
        super().start()

    async def on_heartbeat(self, message):
        if (self.have_append or
            (not self.skip_to_append and not self.send_higher_term_on_hb)):
            return await super().on_heartbeat(message)
        self.logger.debug("\n\n\tintercepting heartbeat to force append" \
                          " to be first call\n\n")
        await self.leaderless_timer.reset()
        if self.send_higher_term_on_hb:
            # only do it once
            term = message.term + 1
            self.send_higher_term_on_hb = False
            # let next heartbeat through
            self.skip_to_append = False
        else:
            term = self.log.get_last_term()
        data = dict(success=False,
                    prevLogIndex=message.prevLogIndex,
                    prevLogTerm=message.prevLogTerm,
                    last_index=self.log.get_last_index(),
                    last_term=self.log.get_last_term())
        reply = HeartbeatResponseMessage(message.receiver,
                                         message.sender,
                                         term=term,
                                         data=data)
        msg = "Sending reject on heartbeat to force append first" \
                  " leader index=%d, term=%d, local index=%d, term = %d"
        self.logger.debug(msg,
                          message.prevLogIndex,
                          message.prevLogTerm,
                          self.log.get_last_index(),
                          term)
        await self.server.post_message(reply)
        return True

    async def on_append_entries(self, message):
        self.have_append = True
        if  self.send_higher_term_on_ae:
            # only do it once
            self.send_higher_term_on_ae = False
            term = message.term + 1
            self.log.set_term(term)
            return await super().on_append_entries(message)
        return await super().on_append_entries(message)
        
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
        self.skip_to_append = False
        self.send_higher_term_on_hb = False
        self.send_higher_term_on_ae = False

    def set_skip_to_append(self, flag):
        self.skip_to_append = flag
    
    def set_send_higher_term_on_hb(self, flag):
        self.send_higher_term_on_hb = flag
        
    def set_send_higher_term_on_ae(self, flag):
        self.send_higher_term_on_ae = flag
        
    async def new_state(self, state_map, old_state, new_state):
        new_state = await super().new_state(state_map, old_state, new_state)
        if new_state.code == StateCode.follower:
            self.state = ModFollower(new_state.server,
                                         new_state.timeout)
            self.state.set_skip_to_append(self.skip_to_append)
            self.state.set_send_higher_term_on_hb(self.send_higher_term_on_hb)
            self.state.set_send_higher_term_on_ae(self.send_higher_term_on_ae)
            return self.state
        return new_state


class TestRareMessages(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.total_nodes = 3
        self.cluster = PausingServerCluster(server_count=self.total_nodes,
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

    def pause_waiter(self, label, expected=None, timeout=2):
        if expected is None:
            expected = self.total_nodes
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
        
    def preamble(self, num_to_start=None):
        self.servers = self.cluster.prepare()
        if num_to_start is None:
            num_to_start = len(self.servers)
        self.pausers = {}
        self.leader = None
        self.followers = []
        self.expected_followers = num_to_start - 1
        for spec in self.servers.values():
            self.pausers[spec.name] = Pauser(spec, self)
        
        self.set_hb_intercept()

        started_count = 0
        for spec in self.cluster.get_servers().values():
            self.cluster.start_one_server(spec.name)
            started_count += 1
            if started_count == num_to_start:
                break

        self.pause_waiter("waiting for pause first election done (heartbeat)",
                          expected = started_count)

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
        self.preamble(num_to_start=2)
        leader = None
        first = None
        second = None
        pos = 0
        for spec in self.cluster.get_servers().values():
            if pos < 2:
                self.assertTrue(spec.running)
                if str(spec.server_obj.state_map.state) == "leader":
                    leader = spec
                else:
                    first = spec
            else:
                self.assertFalse(spec.running)
                second = spec
            pos += 1
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        log_record_count = 1
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count += 1
        self.assertEqual(res['balance'], 10)
        self.postamble()

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
        
    def test_append_first(self):
        # gets branch in follower that happens when the first
        # RPC from the leader is an AppendEntries with entries
        # rather than a heartbeat
        self.preamble(num_to_start=2)
        leader = None
        first = None
        second = None
        pos = 0
        for spec in self.cluster.get_servers().values():
            if pos < 2:
                self.assertTrue(spec.running)
                if str(spec.server_obj.state_map.state) == "leader":
                    leader = spec
                else:
                    first = spec
            else:
                self.assertFalse(spec.running)
                second = spec
            pos += 1
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.do_credit(10)
        log_record_count = 1
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count += 1
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        start_time = time.time()
        tlog = first.server_obj.get_log()
        while time.time() - start_time < 4:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_skip_to_append(True)
        self.cluster.start_one_server(second.name)
        start_time = time.time()
        tlog = second.server_obj.get_log()
        # sometimes elections are slow
        while time.time() - start_time < 8:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
    def test_leader_low_term_hb(self):
        # There is a possible sequence after network partition
        # heals where current leader sends a Heartbeat message
        # to a newly rejoined follower that was briefly leader,
        # then got isolated, so it has a higher term value. Just
        # think of all the possibilities of a buggy network and you'll
        # see that you cannot be sure it would never happen, so
        # the raft algorythm says leader needs to look for that.
        # Look at the "all servers" part of the "rules for servers" on
        # page 4 of the raft.pdf file.

        self.preamble(num_to_start=2)
        leader = None
        first = None
        second = None
        pos = 0
        for spec in self.cluster.get_servers().values():
            if pos < 2:
                self.assertTrue(spec.running)
                if str(spec.server_obj.state_map.state) == "leader":
                    leader = spec
                else:
                    first = spec
            else:
                self.assertFalse(spec.running)
                second = spec
            pos += 1
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        log_record_count = 1
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count += 1
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        start_time = time.time()
        tlog = first.server_obj.get_log()
        while time.time() - start_time < 4:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        # save the leader state object, it should be a different
        # one after this startup sequence, though it may end up the
        # leader again
        orig_leader_state = leader.server_obj.state_map.state
        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_send_higher_term_on_hb(True)
        self.cluster.start_one_server(second.name)
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)

        start_time = time.time()
        tlog = second.server_obj.get_log()
        while time.time() - start_time < 4:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        self.assertNotEqual(orig_leader_state,
                             leader.server_obj.state_map.state)
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
    def test_leader_low_term_ae(self):
        # There is a possible sequence after network partition
        # heals where current leader sends an AppendEntries message
        # to a newly rejoined follower that was briefly leader,
        # then got isolated, so it has a higher term value. Just
        # think of all the possibilities of a buggy network and you'll
        # see that you cannot be sure it would never happen, so
        # the raft algorythm says leader needs to look for that.
        # Look at the "all servers" part of the "rules for servers" on
        # page 4 of the raft.pdf file.

        self.preamble(num_to_start=2)
        leader = None
        first = None
        second = None
        pos = 0
        for spec in self.cluster.get_servers().values():
            if pos < 2:
                self.assertTrue(spec.running)
                if str(spec.server_obj.state_map.state) == "leader":
                    leader = spec
                else:
                    first = spec
            else:
                self.assertFalse(spec.running)
                second = spec
            pos += 1
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        log_record_count = 1
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count += 1
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        start_time = time.time()
        tlog = first.server_obj.get_log()
        while time.time() - start_time < 4:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        # save the leader state object, it should be a different
        # one after this startup sequence, though it may end up the
        # leader again
        orig_leader_state = leader.server_obj.state_map.state
        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_send_higher_term_on_ae(True)
        self.cluster.start_one_server(second.name)
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)

        start_time = time.time()
        tlog = second.server_obj.get_log()
        while time.time() - start_time < 4:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        self.assertNotEqual(orig_leader_state,
                            leader.server_obj.state_map.state)
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
