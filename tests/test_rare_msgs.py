import unittest
import asyncio
import time
import logging
import traceback
import os

from tests.common_tcase import TestCaseCommon

from raftframe.messages.heartbeat import HeartbeatResponseMessage
from raftframe.states.base_state import StateCode
from raftframe.states.follower import Follower
from dev_tools.pserver import PauseSupportMonitor

class ModFollower(Follower):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.have_append = False
        self.skip_to_append = False
        self.send_higher_term_on_hb = False
        self.send_higher_term_on_ae = False
        self.never_beat = False

    def set_skip_to_append(self, flag):
        self.skip_to_append = flag
    
    def set_send_higher_term_on_hb(self, flag):
        self.send_higher_term_on_hb = flag

    def set_send_higher_term_on_ae(self, flag):
        self.send_higher_term_on_ae = flag

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
        
class ModMonitor(PauseSupportMonitor):

    
    def __init__(self, orig_monitor):
        super().__init__(orig_monitor.pserver)
        self.state_map = self.pserver.state_map
        self.state = self.state_map.state
        self.substate = self.state_map.substate
        self.state_map.remove_state_change_monitor(orig_monitor)
        self.state_map.add_state_change_monitor(self)
        self.skip_to_append = False
        self.send_higher_term_on_hb = False
        self.send_higher_term_on_ae = False
        self.never_beat = False

    def set_skip_to_append(self, flag):
        self.skip_to_append = flag
    
    def set_send_higher_term_on_hb(self, flag):
        self.send_higher_term_on_hb = flag
        
    def set_send_higher_term_on_ae(self, flag):
        self.send_higher_term_on_ae = flag
        
    def set_never_beat(self, flag):
        self.never_beat = flag
    
    async def new_state(self, state_map, old_state, new_state):
        self.state = self.state_map.get_state()
        self.substate = self.state_map.get_substate()
        new_state = await super().new_state(state_map, old_state, new_state)
        if new_state.code == StateCode.follower:
            self.state = ModFollower(new_state.server,
                                         new_state.timeout)
            self.state.set_skip_to_append(self.skip_to_append)
            self.state.set_send_higher_term_on_hb(self.send_higher_term_on_hb)
            self.state.set_send_higher_term_on_ae(self.send_higher_term_on_ae)
            self.state.set_never_beat(self.never_beat)
            return self.state
        return new_state

class TestRareMessages(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestRareMessages, cls).setUpClass()
        cls.total_nodes = 3
        cls.timeout_basis = 0.5
        if __name__.startswith("tests"):
            cls.logger_name = __name__
        else:
            cls.logger_name = "tests." + __name__

    def setup(self):
        super().setup()
        self.addCleanup(self.cleanup)
        
    def cleanup(self):
        print("\n\n\n\n IN CLEANUP \n\n\n\n")
        
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
        self.preamble(num_to_start=2)
        for pserver in self.cluster.servers:
            if pserver != self.leader:
                if pserver.running:
                    first = pserver
                else:
                    second = pserver

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.clear_intercepts()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        tlog = first.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.thread.server.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_skip_to_append(True)
        second.clear_substate_pauses()
        second.start()
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        # sometimes elections are slow
        start_time = time.time()
        while time.time() - start_time < 20 * self.timeout_basis:
            if second.thread and second.thread.server:
                tlog = second.thread.server.get_log()
                if tlog.get_commit_index() == log_record_count:
                    break
                tlog = second.thread.server.get_log()
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
        # the raftframe algorythm says leader needs to look for that.
        # Look at the "all servers" part of the "rules for servers" on
        # page 4 of the raftframe.pdf file.

        # Preamble starts 2 of 3 servers
        # (num_to_start arg of self.total_nodes) and
        # waits for them to complete the leader election.
        # When it returns, the servers are paused at the point
        # where the leader has sent the initial AppendEntries
        # message containing the no-op log record that marks
        # the beginning of the term. The followers have not
        # yet processed it, are paused before delivery of the
        # message to the follower code.

        self.preamble(num_to_start=2)
        for spec in self.cluster.get_servers().values():
            if spec != self.leader:
                if spec.running:
                    first = spec
                else:
                    second = spec
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        log_record_count = self.leader.server_obj.get_log().get_last_index()
        tlog = first.server_obj.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        # save the leader state object, it should be a different
        # one after this startup sequence, though it may end up the
        # leader again
        orig_leader_state = self.leader.server_obj.state_map.state
        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        old_leader_obj = self.leader.server_obj.state_map.state
        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_send_higher_term_on_hb(True)
        self.cluster.start_one_server(second.name)
        self.logger.debug("\n\n\tAwaiting new leader \n")
        new_leader = None
        start_time = time.time()
        # elections can take many tries
        while time.time() - start_time < 30 * self.timeout_basis:
            for spec in self.cluster.get_servers().values():
                state_obj = spec.server_obj.state_map.state
                if str(state_obj) == "leader":
                    if state_obj != old_leader_obj:
                        new_leader = spec
                        break
            if new_leader:
                break
            time.sleep(0.01)
        self.assertIsNotNone(new_leader)
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)
        start_time = time.time()
        tlog = second.server_obj.get_log()
        while time.time() - start_time < 4:
            log_record_count = new_leader.server_obj.get_log().get_last_index()
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        self.assertNotEqual(orig_leader_state,
                            self.leader.server_obj.state_map.state)
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
        # the raftframe algorythm says leader needs to look for that.
        # Look at the "all servers" part of the "rules for servers" on
        # page 4 of the raftframe.pdf file.

        # Preamble starts 2 of 3 servers
        # (num_to_start arg of self.total_nodes) and
        # waits for them to complete the leader election.
        # When it returns, the servers are paused at the point
        # where the leader has sent the initial AppendEntries
        # message containing the no-op log record that marks
        # the beginning of the term. The followers have not
        # yet processed it, are paused before delivery of the
        # message to the follower code.

        self.preamble(num_to_start=2)
        for spec in self.cluster.get_servers().values():
            if spec != self.leader:
                if spec.running:
                    first = spec
                else:
                    second = spec
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        log_record_count = self.leader.server_obj.get_log().get_last_index()
        tlog = first.server_obj.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        # save the leader state object, it should be a different
        # one after this startup sequence, though it may end up the
        # leader again
        old_leader_obj = self.leader.server_obj.state_map.state
        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        # make it claim to have a higher term
        second.monitor.set_send_higher_term_on_ae(True)
        self.cluster.start_one_server(second.name)
        self.logger.debug("\n\n\tElection due to low term at leader\n")
        new_leader = None
        start_time = time.time()
        # election can be slow, depending on random candidate
        # timeouts
        while time.time() - start_time < 30 * self.timeout_basis:
            for spec in self.cluster.get_servers().values():
                state_obj = spec.server_obj.state_map.state
                if str(state_obj) == "leader":
                    if state_obj != old_leader_obj:
                        new_leader = spec
                        break
            if new_leader:
                break
            time.sleep(0.1)
        self.assertIsNotNone(new_leader)
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)
        # New leader will put a no-op record in the log on startup
        tlog = second.server_obj.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            log_record_count = new_leader.server_obj.get_log().get_last_index()
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
    def test_backdown(self):
        # If a follower starts up and has some, but not all of the
        # log entries that the leader has, then the leader should
        # find out via a reply from the heartbeat or append entries
        # message when the follower says that it does not have the
        # previous message specified. We are looking specifically
        # for the AppendEntries flavor of this, which only happens
        # in the case that the leader sends that before it sends
        # a heartbeat right after the follower starts. We will
        # force that by setting up the relevant log condition in
        # a follower, stop it, add log to the leader, then restart
        # the follower but intercept and block the heartbeat and then
        # add another log record in the normal fashion. This will
        # cause the AppendEntries message for the new record to
        # get the "I'm not ready" response that we are looking for.

        # Preamble starts 3 servers (self.total_nodes) and
        # waits for them to complete the leader election.
        # When it returns, the servers are paused at the point
        # where the leader has sent the initial AppendEntries
        # message containing the no-op log record that marks
        # the beginning of the term. The followers have not
        # yet processed it, are paused before delivery of the
        # message to the follower code.

        self.preamble()
        first = self.non_leaders[0]
        second = self.non_leaders[1]
        
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        log_record_count = self.leader.server_obj.get_log().get_last_index()
        tlog = first.server_obj.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        self.logger.debug("\n\n\tStopping third server %s \n\n",
                          second.name)

        self.cluster.stop_server(second.name)
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 20)

        self.logger.debug("\n\n\tRestarting third server %s \n\n",
                          second.name)

        second = self.cluster.prepare_one(second.name)
        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_never_beat(True)
        self.cluster.start_one_server(second.name)
        
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)

        self.logger.debug("\n\n\tCredit 10 to %s \n\n", self.leader.name)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count = self.leader.server_obj.get_log().get_last_index()
        
        self.assertEqual(res['balance'], 30)
        tlog = second.server_obj.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        second.monitor.set_never_beat(False)
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
        self.preamble()
        
        first = self.non_leaders[0]
        second = self.non_leaders[1]
        
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        log_record_count = self.leader.server_obj.get_log().get_last_index()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit
        tlog = first.server_obj.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        self.logger.debug("\n\n\tStopping third server %s \n\n",
                          second.name)

        self.cluster.stop_server(second.name)
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 20)

        self.logger.debug("\n\n\tRestarting third server %s \n\n",
                          second.name)

        second = self.cluster.prepare_one(second.name)
        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_never_beat(True)
        self.cluster.start_one_server(second.name)
        
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)

        self.logger.debug("\n\n\tCredit 10 to %s \n\n", self.leader.name)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 30)
        log_record_count = self.leader.server_obj.get_log().get_last_index()
        tlog = second.server_obj.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.server_obj.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        second.monitor.set_never_beat(False)
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
