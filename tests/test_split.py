import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path

from raftframe.messages.heartbeat import HeartbeatMessage
from raftframe.states.base_state import StateCode, Substate
from tests.common_tcase import TestCaseCommon

class TestSplit(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestSplit, cls).setUpClass()
        # All the tests in the case use a cluster of 5 servers
        # due to this override
        cls.total_nodes = 5
        # Need to go a little slower with 5 servers than 3
        cls.timeout_basis = 0.2
        if __name__.startswith("tests"):
            cls.logger_name = __name__
        else:
            cls.logger_name = "tests." + __name__

    def set_hb_intercept(self, clear=True):

        for pserver in self.cluster.servers:
            if clear:
                pserver.clear_message_triggers()
                pserver.clear_state_pauses()
                pserver.clear_substate_pauses()
            pserver.pause_on_substate(Substate.sent_heartbeat)
            pserver.pause_before_in_message(HeartbeatMessage.get_code())
            
    def test_leader_stays_split(self):
        # Preamble starts 5 servers (self.total_nodes) and
        # waits for them to complete the leader election.
        # When it returns, the servers are paused at the point
        # where the leader has sent the initial AppendEntries
        # message containing the no-op log record that marks
        # the beginning of the term. The followers have not
        # yet processed it, are paused before delivery of the
        # message to the follower code.
        self.preamble()
        self.clear_pause_triggers()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
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

        lost_1.comms.set_partition(1)
        lost_2.comms.set_partition(1)
        
        self.clear_pause_triggers()
        self.cluster.resume_all()

        self.logger.debug("\n\n\tAdding 10 via do_credit\n\n")
        client.do_credit(10)
        log_record_count = self.leader.thread.server.get_log().get_last_index()
        start_time = time.time()
        while time.time() - start_time < 4:
            done = []
            for pserver in [leader, kept_1, kept_2]:
                tlog = pserver.thread.server.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(pserver)
            
            if len(done) == 3:
                break
        self.assertEqual(len(done), 3, msg="Log commits missing for 4 seconds")
        for pserver in [lost_1, lost_2]:
            tlog = pserver.thread.server.get_log()
            # log indices are 1 offset, not zero
            self.assertTrue(tlog.get_commit_index() < log_record_count)
        self.logger.debug("\n\n\tHealing fake network partition\n\n")
        lost_1.comms.set_partition(0)
        lost_2.comms.set_partition(0)
        
        start_time = time.time()
        while time.time() - start_time < 4:
            done = []
            for pserver in [lost_1, lost_2]:
                tlog = pserver.thread.server.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(pserver)
            
            if len(done) == 2:
                break
        self.assertEqual(len(done), 2,
                         msg="Post heal log commits missing for 4 seconds")
        
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
    def test_leader_lost_split(self):
        self.preamble()
        leader = self.leader
        lost_1 = self.non_leaders[0]
        kept_1 = self.non_leaders[1]
        kept_2 = self.non_leaders[2]
        kept_3 = self.non_leaders[3]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 \n\n")
        client = self.leader.get_client()
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        time.sleep(0.01) # allow time for the commit messages to flow

        log_record_count = self.leader.thread.server.get_log().get_last_index()
        start_time = time.time()
        while time.time() - start_time < 2:
            done = []
            for pserver in self.cluster.servers:
                tlog = pserver.thread.server.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(pserver)
            if len(done) == 5:
                break
            time.sleep(0.1)
        self.assertEqual(len(done), 5,
                         msg="Post heal log commits missing for 2 seconds")

        self.set_hb_intercept()
        self.wait_till_paused(5)

        self.logger.debug("\n\n\tIsolating two servers as though" \
                          " network partitioned, leader %s and %s\n\n",
                          leader.name, lost_1.name)

        leader.comms.set_partition(1)
        lost_1.comms.set_partition(1)

        self.clear_pause_triggers()
        self.cluster.resume_all()
        new_leader = None
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            for pserver in [kept_1, kept_2, kept_3]:
                if str(pserver.thread.server.state_map.state) == "leader":
                    new_leader = pserver
                    break
            if new_leader:
                break
            time.sleep(0.1)
        self.assertIsNotNone(new_leader)
        
        term = new_leader.thread.server.get_log().get_term()
        done = []
        start_time = time.time()
        while time.time() - start_time < 5 * self.timeout_basis:
            done = []
            for pserver in [kept_1, kept_2, kept_3]:
                tlog = pserver.thread.server.get_log()
                if tlog.get_term() == term:
                    done.append(pserver)
            if len(done) == 3:
                break
            time.sleep(0.1)
        self.assertEqual(len(done), 3)
        self.logger.debug("\n\n\tGot New leader %s," \
                          " Adding 10 via do_credit\n\n",
                          new_leader.name)
        client2 = new_leader.get_client()
        client2.do_credit(10)
        start_time = time.time()
        while time.time() - start_time < 10 * self.timeout_basis:
            log_record_count = new_leader.thread.server.get_log().get_last_index()
            done = []
            for pserver in [kept_1, kept_2, kept_3]:
                tlog = pserver.thread.server.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(pserver)
            if len(done) == 3:
                break
            time.sleep(0.1)
        msg=f"Log commits missing for {10 * self.timeout_basis} seconds"
        self.assertEqual(len(done), 3, msg)
        for pserver in [leader, lost_1]:
            tlog = pserver.thread.server.get_log()
            # log indices are 1 offset, not zero
            self.assertTrue(tlog.get_commit_index() < log_record_count)
        self.logger.debug("\n\n\tHealing fake network partition\n\n")
        leader.comms.set_partition(0)
        lost_1.comms.set_partition(0)
        for pserver in self.cluster.servers:
            if pserver in [leader, lost_1]:
                pserver.clear_message_triggers()
                pserver.resume()
        
        start_time = time.time()
        while time.time() - start_time <  10 * self.timeout_basis:
            done = []
            for pserver in [leader, lost_1]:
                tlog = pserver.thread.server.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(pserver)
            
            if len(done) == 2:
                break
            time.sleep(0.1)
        msg="Post heal log commits missing for " \
            f"{10 * self.timeout_basis} seconds"
        self.assertEqual(len(done), 2,
                         msg=msg)
        
        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
    def wait_till_paused(self, expected=None):
        if expected is None:
            expected = self.total_nodes
        paused = []
        start_time = time.time()
        while time.time() - start_time < 3 * self.timeout_basis:
            paused = []
            for server in self.cluster.servers:
                if server.paused:
                    paused.append(server)
            if len(paused) == expected:
                break
            time.sleep(0.1)
        self.assertEqual(len(paused), expected)

