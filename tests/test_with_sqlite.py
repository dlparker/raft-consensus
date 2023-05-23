import unittest
import asyncio
import time
import logging
import traceback
import os

from tests.common_tcase import TestCaseCommon

from raftframe.messages.heartbeat import HeartbeatResponseMessage
from raftframe.states.base_state import StateCode
from raftframe.log.sqlite_log import SqliteLog
from dev_tools.pausing_app import PausingMonitor, PLeader, PFollower


class ModFollower(PFollower):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.have_append = False
        self.skip_to_append = False
        self.send_higher_term_on_hb = False
        self.send_higher_term_on_ae = False
        self.lie_about_index = False
        self.never_beat = False

    def set_skip_to_append(self, flag):
        self.skip_to_append = flag
    
    def set_send_higher_term_on_hb(self, flag):
        self.send_higher_term_on_hb = flag

    def set_send_higher_term_on_ae(self, flag):
        self.send_higher_term_on_ae = flag

    def set_lie_about_index(self, flag):
        self.lie_about_index = flag

    def start(self):
        super().start()

    def set_never_beat(self, flag):
        self.never_beat = flag
        
    async def on_heartbeat(self, message):
        intercept = False
        term = self.log.get_last_term()
        last_index = self.log.get_last_index()
        last_term = self.log.get_last_term()
        if self.never_beat:
            self.logger.debug("\n\n\tNot applying heartbeat at all")
            await self.leaderless_timer.reset()
            return True
        if self.skip_to_append and not self.have_append:
            intercept = True
            success = False
        if self.send_higher_term_on_hb and not self.have_append:
            success = False
            intercept = True
            # do it once only
            self.send_higher_term_on_hb = False
            term = message.term + 1
        if self.lie_about_index:
            success = False
            # do it once only
            self.lie_about_index = False
            last_index += 1
            last_term += 1
        if not intercept:
            return await super().on_heartbeat(message)
        self.logger.debug("\n\n\tintercepting heartbeat to force append" \
                          " to be first call\n\n")
        await self.leaderless_timer.reset()
        data = dict(success=success,
                    prevLogIndex=message.prevLogIndex,
                    prevLogTerm=message.prevLogTerm,
                    last_index=last_index,
                    last_term=last_term)
        reply = HeartbeatResponseMessage(message.receiver,
                                         message.sender,
                                         term=term,
                                         data=data)
        msg = "Sending reject on heartbeat to force append first" \
                  " leader index=%d, term=%d, local index=%d, term = %d"
        self.logger.debug(msg,
                          message.prevLogIndex,
                          message.prevLogTerm,
                          last_term,
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
        self.lie_about_index = False
        self.never_beat = False

    def set_skip_to_append(self, flag):
        self.skip_to_append = flag
    
    def set_send_higher_term_on_hb(self, flag):
        self.send_higher_term_on_hb = flag
        
    def set_send_higher_term_on_ae(self, flag):
        self.send_higher_term_on_ae = flag
        
    def set_lie_about_index(self, flag):
        self.lie_about_index = flag

    def set_never_beat(self, flag):
        self.never_beat = flag
    
    async def new_state(self, state_map, old_state, new_state):
        new_state = await super().new_state(state_map, old_state, new_state)
        if new_state.code == StateCode.follower:
            self.state = ModFollower(new_state.server,
                                         new_state.timeout)
            self.state.set_skip_to_append(self.skip_to_append)
            self.state.set_send_higher_term_on_hb(self.send_higher_term_on_hb)
            self.state.set_send_higher_term_on_ae(self.send_higher_term_on_ae)
            self.state.set_lie_about_index(self.lie_about_index)
            self.state.set_never_beat(self.never_beat)
            return self.state
        return new_state


class TestRestarts(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestRestarts, cls).setUpClass()
        cls.total_nodes = 3
        cls.timeout_basis = 0.1
        if __name__.startswith("tests"):
            cls.logger_name = __name__
        else:
            cls.logger_name = "tests." + __name__

    def change_log(self):
        for spec in self.cluster.get_servers().values():
            spec.pbt_server.data_log = SqliteLog(f"/tmp/raft_tests/{spec.name}")
            
    # All the tests in the case use a cluster of three servers
    # as configured by the TestCaseCommon setUp method.
    def test_append_first(self):
        # Preamble starts 2 of 3 servers
        # (num_to_start arg of self.total_nodes) and
        # waits for them to complete the leader election.
        # When it returns, the servers are paused at the point
        # where the leader has sent the initial AppendEntries
        # message containing the no-op log record that marks
        # the beginning of the term. The followers have not
        # yet processed it, are paused before delivery of the
        # message to the follower code.
        self.preamble(num_to_start=2, pre_start_callback=self.change_log)
        for spec in self.cluster.get_servers().values():
            if spec != self.leader:
                if spec.running:
                    first = spec
                else:
                    second = spec
        self.logger.debug("\n\n\tPreamble Done\n\n")
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.clear_pause_triggers()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        # log operations from main thread (this one) don't work
        # when using sqlite, as it requires single threaded access.
        # therefore we need to do this dodge
        log_stats = self.leader.pbt_server.get_log_stats()
        log_record_count = log_stats['last_index']
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            f_log_stats = first.pbt_server.get_log_stats()
            if f_log_stats['commit_index'] == log_record_count:
                break
            time.sleep(0.01)
        self.assertEqual(f_log_stats['commit_index'], log_record_count)

        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_skip_to_append(True)
        self.cluster.start_one_server(second.name)
        # log operations from main thread (this one) don't work
        # when using sqlite, as it requires single threaded access.
        # therefore we need to do this dodge
        log_stats = self.leader.pbt_server.get_log_stats()
        log_record_count = log_stats['last_index']
        # sometimes elections are slow
        start_time = time.time()
        while time.time() - start_time < 30 * self.timeout_basis:
            s_log_stats = first.pbt_server.get_log_stats()
            if s_log_stats['commit_index'] == log_record_count:
                break
            time.sleep(0.01)
        self.assertEqual(s_log_stats['commit_index'], log_record_count)
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()

        
    def a_test_new_term_backdown(self):
        # Consider a sequence during which servers crash or become
        # isolated, or "are lost".
        # The leader with term 1 appends a log record to index 2, but before
        # it can send the AppendEntries RPC it dies or is lost.
        # A new leader is elected at term 2, and processes one or more
        # new log records. This leaves the log record at index 2 at the new
        # leader with term 2, whereas the original leader has term 1.
        # The old leader re-joins the cluster. The new leader gets
        # a new log entry before the heartbeat occurs to the old leader,
        # and the old leader detects that the term of the log cursor
        # does not match. The reply to the new leader causes it to
        # backdown to send record 2 to the old leader. It accepts it
        # and replaces its own record 2. The normal catchup sequence
        # then brings old leader into line.

        # Preamble starts 3 servers (self.total_nodes) and
        # waits for them to complete the leader election.
        # When it returns, the servers are paused at the point
        # where the leader has sent the initial AppendEntries
        # message containing the no-op log record that marks
        # the beginning of the term. The followers have not
        # yet processed it, are paused before delivery of the
        # message to the follower code.
        pass
