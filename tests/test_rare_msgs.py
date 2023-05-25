import unittest
import asyncio
import time
import logging
import traceback
import os

from tests.common_tcase import TestCaseCommon

from raftframe.messages.append_entries import AppendEntriesMessage
from raftframe.messages.heartbeat import HeartbeatResponseMessage
from raftframe.states.base_state import StateCode, Substate
from raftframe.states.follower import Follower
from dev_tools.pserver import PauseSupportMonitor

class ModFollower(Follower):

    def __init__(self, *args, **kwargs):
        self.monitor = kwargs.pop('monitor')
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
            self.logger.debug("\n\n\tintercepting heartbeat to force append" \
                          " to be first call\n\n")
            intercept = True
            success = False
        if self.send_higher_term_on_hb and not self.have_append:
            self.logger.debug("\n\n\tintercepting to force higher term on hb\n\n")
            success = False
            intercept = True
            # do it once only
            self.send_higher_term_on_hb = False
            term = message.term + 1
        if not intercept:
            return await super().on_heartbeat(message)
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
            self.logger.debug("inserting higher term value to cause appendEntries reject")
            self.monitor.set_send_higher_term_on_ae(False)
            self.logger.debug("Disabling higher term on appendEntries after one pass")
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
                                     new_state.timeout,
                                     monitor=self)
            self.state.set_skip_to_append(self.skip_to_append)
            self.state.set_send_higher_term_on_hb(self.send_higher_term_on_hb)
            self.state.set_send_higher_term_on_ae(self.send_higher_term_on_ae)
            self.state.set_never_beat(self.never_beat)
            self.state_map.state = self.state
            return self.state
        return new_state

class TestRareMessages(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestRareMessages, cls).setUpClass()
        cls.total_nodes = 3
        cls.timeout_basis = 0.1
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
        self.clear_pause_triggers()
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
        for pserver in self.cluster.servers:
            if pserver != self.leader:
                if pserver.running:
                    first = pserver
                else:
                    second = pserver

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        # clear everything so cluster can start with
        # two servers
        self.clear_pause_triggers()
        self.cluster.resume_all()
        # put some things in the log
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit on last leader log record
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        tlog = first.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.thread.server.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        # save the leader state object, it should be a different
        # one after this startup sequence, though it may end up the
        # leader again
        old_leader_state = self.leader.state_map.get_state()
        old_leader = self.leader
        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_send_higher_term_on_hb(True)
        for server in self.cluster.servers:
            server.clear_substate_pauses()
            server.pause_on_substate(Substate.became_leader)
            server.pause_on_substate(Substate.joined)
        second.start()
        self.logger.debug("\n\n\tAwaiting new leader on rejected higher term in heartbeat\n")
        start_time = time.time()
        # wait for second to start
        while time.time() - start_time < 10 * self.timeout_basis:
            if second.thread and second.thread.server:
                break
            time.sleep(0.01)
        self.assertIsNotNone(second.thread)
        self.assertIsNotNone(second.thread.server)
        self.logger.debug("\n\n\tAwaiting new leader on rejected higher term in heartbeat\n")
        # elections can take many tries
        new_leader = None
        start_time = time.time()
        while time.time() - start_time < 30 * self.timeout_basis:
            for server in self.cluster.servers:
                state_obj = server.state_map.get_state()
                if str(state_obj) == "leader":
                    if state_obj != old_leader_state:
                        new_leader = server
                        break
            if new_leader:
                break
            time.sleep(0.01)
        self.assertIsNotNone(new_leader)
        self.logger.debug(f' old leader {old_leader.name} new leader {new_leader.name}')
        self.cluster.resume_all()
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)
        start_time = time.time()
        tlog = second.thread.server.get_log()
        while time.time() - start_time < 20 * self.timeout_basis:
            log_record_count = new_leader.thread.server.get_log().get_last_index()
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.thread.server.get_log()
            time.sleep(0.01)

        # These are the key checks. The log should be consisent now,
        # and the leadership should have changed.
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        self.assertNotEqual(old_leader_state,
                            self.leader.state_map.get_state())
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
        for pserver in self.cluster.servers:
            if pserver != self.leader:
                if pserver.running:
                    first = pserver
                else:
                    second = pserver

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        # clear everything so cluster can start with
        # two servers
        self.clear_pause_triggers()
        self.cluster.resume_all()
        # put some things in the log
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for follower to have commit on last leader log record
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        tlog = first.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = first.thread.server.get_log()
            time.sleep(0.01)
        self.assertEqual(tlog.get_commit_index(), log_record_count)

        # save the leader state object, it should be a different
        # one after this startup sequence, though it may end up the
        # leader again
        old_leader_state = self.leader.state_map.get_state()
        old_leader = self.leader
        self.logger.debug("\n\n\tStarting third server %s \n\n",
                          second.name)

        second.monitor = ModMonitor(second.monitor)
        second.monitor.set_send_higher_term_on_ae(True)
        for server in self.cluster.servers:
            server.clear_substate_pauses()
        second.start()
        self.logger.debug("\n\n\tAwaiting new leader on rejected higher term in add entry\n")
        new_leader = None
        start_time = time.time()
        # wait for second to start
        while time.time() - start_time < 10 * self.timeout_basis:
            if second.thread and second.thread.server:
                break
            time.sleep(0.01)
        self.assertIsNotNone(second.thread)
        self.assertIsNotNone(second.thread.server)
        # elections can take many tries
        start_time = time.time()
        while time.time() - start_time < 30 * self.timeout_basis:
            for server in self.cluster.servers:
                state_obj = server.state_map.get_state()
                if str(state_obj) == "leader":
                    if state_obj != old_leader_state:
                        new_leader = server
                        break
            if new_leader:
                break
            time.sleep(0.01)
        self.assertIsNotNone(new_leader)
        self.logger.debug(f'old leader {old_leader.name} new leader {new_leader.name}')

        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)
        start_time = time.time()
        tlog = second.thread.server.get_log()
        while time.time() - start_time < 20 * self.timeout_basis:
            log_record_count = new_leader.thread.server.get_log().get_last_index()
            if tlog.get_commit_index() == log_record_count:
                break
            tlog = second.thread.server.get_log()
            time.sleep(0.01)

        # These are the key checks. The log should be consisent now,
        # and the leadership should have changed.
        self.assertEqual(tlog.get_commit_index(), log_record_count)
        self.assertNotEqual(old_leader_state,
                            self.leader.state_map.get_state())
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
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
        self.clear_pause_triggers()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        # wait for both followers to have commit
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        f_log = first.thread.server.get_log()
        s_log = second.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if (f_log.get_commit_index() == log_record_count
                and s_log.get_commit_index() == log_record_count):
                break
            f_log = first.thread.server.get_log()
            s_log = second.thread.server.get_log()
            time.sleep(0.01)
        self.assertEqual(f_log.get_commit_index(), log_record_count)
        self.assertEqual(s_log.get_commit_index(), log_record_count)

        # Stop the second follower then add a log record that the
        # stopped one won't have
        self.logger.debug("\n\n\tStopping second follower server %s \n\n",
                          second.name)
        second.stop()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 20)

        self.logger.debug("\n\n\tRestarting second follower server %s \n\n",
                          second.name)

        second = self.cluster.regen_server(second)
        second.monitor = ModMonitor(second.monitor)
        # block the heartbeat, forcing the first message that
        # the follower gets to be addEntry, which will then
        # note that the follower is behind, causeing the leader
        # to backdown to previous entry.
        second.monitor.set_never_beat(True)
        second.start()
        self.logger.debug("\n\n\tAwaiting log update at %s\n",
                          second.name)

        # Now make some log updates, which should force the above
        # described sequence
        self.logger.debug("\n\n\tCredit 10 to %s \n\n", self.leader.name)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 30)
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        
        # check the actual log in the follower
        s_log = second.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if s_log.get_commit_index() == log_record_count:
                break
            s_log = second.thread.server.get_log()
            time.sleep(0.01)
        self.assertEqual(s_log.get_commit_index(), log_record_count)
        # clear the heartbeat reject, just to allow clean shutdown
        second.monitor.set_never_beat(False)
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        self.postamble()
                      
    def test_new_term_backdown(self):
        # Consider a sequence during which servers crash or become
        # isolated, or "are lost".  The leader with term 1 appends a
        # log record to a new index value, but before it can send the
        # AppendEntries RPC it dies or is lost.  A new leader is
        # elected at term 2, and processes one or more new log
        # records. This leaves the log record at index 2 at the new
        # leader with term 2, whereas the original leader has term 1.
        # The old leader re-joins the cluster. The new leader gets a
        # new log entry before the heartbeat occurs to the old leader,
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
        self.clear_pause_triggers()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(5)
        self.logger.debug("\n\n\tCredit 5 \n\n")
        client.do_credit(10)
        
        # wait for both followers to have last commit
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        f_log = first.thread.server.get_log()
        s_log = second.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if (f_log.get_commit_index() == log_record_count
                and s_log.get_commit_index() == log_record_count):
                break
            f_log = first.thread.server.get_log()
            s_log = second.thread.server.get_log()
            time.sleep(0.01)
        self.assertEqual(f_log.get_commit_index(), log_record_count)
        self.assertEqual(s_log.get_commit_index(), log_record_count)


        # Now set up the scenario where the leader gets a new entry and
        # dies before it sends appendEntries rpc, and then rejoins
        # cluster after a new leader is elected. We'll simulate that
        # by pausing the leader and resuming the followers. That will
        # also preserve the leader's log (as opposed to a restart)
        # since it is in memory and cannot be recovered on restart.
        old_leader = self.leader
        old_leader_state = self.leader.state_map.get_state()
        old_leader.pause_before_out_message(AppendEntriesMessage._code)

        client.set_timeout(0.1)
        with self.assertRaises(Exception) as context:
            client.do_credit(15)
        client.set_timeout(1)

        new_leader = None
        # elections can take many tries
        start_time = time.time()
        while time.time() - start_time < 30 * self.timeout_basis:
            for server in self.cluster.servers:
                state_obj = server.state_map.get_state()
                if str(state_obj) == "leader":
                    if state_obj != old_leader_state:
                        new_leader = server
                        break
            if new_leader:
                break
            time.sleep(0.01)
        self.assertIsNotNone(new_leader)
        self.leader = new_leader
        # have a new leader, so make a new log record
        
        self.logger.debug("\n\n\tCredit 10 \n\n")
        # get client for new leader
        client = self.leader.get_client()
        client.do_credit(20)

        # Ensure that old leader learns about new entry
        # from appendEntries, not hearbeat, by ensuring
        # that it does not accept hearbeat. Pause it before
        # processing appendEntries, to check test conditions
        self.clear_pause_triggers()
        old_leader.pause_before_in_message(AppendEntriesMessage._code)
        old_leader.monitor = ModMonitor(old_leader.monitor)
        old_leader.monitor.set_never_beat(False)
        old_leader.resume()

        # new entry so that old leader gets told
        # that it is not leader anymore on appendEntries
        # RPC
        self.logger.debug("\n\n\tCredit 10 to new leader %s, "
                          "should trigger appendEntries to old leader %s\n\n",
                          self.leader.name, old_leader.name)
        client.do_credit(25)
        
        start_time = time.time()
        while time.time() - start_time < 3 * self.timeout_basis:
            if old_leader.paused:
                break
            time.sleep(0.1)
        self.assertTrue(old_leader.paused)
        # pause the others while we check, not strictly needed
        self.cluster.pause_all()
        self.wait_till_paused()
        
        leader_log = self.leader.thread.server.get_log()
        old_leader_log = old_leader.thread.server.get_log()
        # If test conditions are correct, then
        #   1. the leader's term should be more than old leader's
        #   2. the index of the last item in the leader's log should
        #      two more than the last index of the old leader's log
        #   3. the last record in the old leader's log should have
        #      the old term, but the record with the same index
        #      in the new leader is the new term, with diffent contents.
        #   
        # The terms should
        self.assertTrue(leader_log.get_term() > old_leader_log.get_term())
        old_last_record = old_leader_log.read()
        new_last_record = leader_log.read()
        # it should be two more, because the new term nop record should
        # overwrite the uncommitted record in the old leader's log, and
        # then we should have the committed record of credit 20, then
        # the record of credit 30, which may or not be committed depending
        # on timing, probably is.
        conflict_index = old_last_record.index
        self.assertEqual(conflict_index + 2, new_last_record.index)
        new_conflict_record = leader_log.read(conflict_index)
        self.assertNotEqual(old_last_record.term, new_conflict_record.term)
        self.assertFalse(old_last_record.committed)
        self.assertTrue(new_conflict_record.committed)
        credit20rec = leader_log.read(conflict_index + 1)
        credit25rec = leader_log.read(conflict_index + 2)
        self.assertTrue(credit20rec.committed)
        self.assertNotEqual(old_last_record.user_data, new_conflict_record.user_data)

        # Now when we resume the servers, old_leader should send back a reply
        # saying that it does not match the appendEntries conditions, so the
        # leader should re-formulate, backing down to the last matching record
        # and send all the missing records in another appendEntries message.
        old_leader.clear_message_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)

        # wait for old leader to have last commit
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        o_log = old_leader.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            if (o_log.get_commit_index() == log_record_count
                and s_log.get_commit_index() == log_record_count):
                break
            o_log = old_leader.thread.server.get_log()
            time.sleep(0.01)
        self.assertEqual(o_log.get_commit_index(), log_record_count)

        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        self.postamble()

    def wait_till_paused(self):
        start_time = time.time()
        paused = []
        while time.time() - start_time < 3 * self.timeout_basis:
            paused = []
            for server in self.cluster.servers:
                if server.paused:
                    paused.append(server)
            if len(paused) == 3:
                break
            time.sleep(0.1)
        self.assertEqual(len(paused), 3)
        
