import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path

from raft.tests.common_tcase import TestCaseCommon

class TestSplit(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestSplit, cls).setUpClass()
        # All the tests in the case use a cluster of 5 servers
        # due to this override
        cls.total_nodes = 5
        # Need to go a little slower with 5 servers than 3
        cls.timeout_basis = 0.2
        if __name__.startswith("raft.tests"):
            cls.logger_name = __name__
        else:
            cls.logger_name = "raft.tests." + __name__
        
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
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.resume_waiter()
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

        lost_1.pbt_server.comms.set_partition(1)
        lost_2.pbt_server.comms.set_partition(1)
        
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()

        self.logger.debug("\n\n\tAdding 10 via do_credit\n\n")
        client.do_credit(10)
        log_record_count = self.leader.server_obj.get_log().get_last_index()
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
        self.logger.debug("\n\n\tQuery to %s \n\n", self.leader.name)
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        time.sleep(0.01) # allow time for the commit messages to flow

        log_record_count = self.leader.server_obj.get_log().get_last_index()
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
        while time.time() - start_time < 10 * self.timeout_basis:
            for spec in [kept_1, kept_2, kept_3]:
                if str(spec.server_obj.state_map.state) == "leader":
                    new_leader = spec
                    break
            if new_leader:
                break
            time.sleep(0.1)
        self.assertIsNotNone(new_leader)
        
        term = new_leader.server_obj.get_log().get_term()
        done = []
        start_time = time.time()
        while time.time() - start_time < 5 * self.timeout_basis:
            done = []
            for spec in [kept_1, kept_2, kept_3]:
                tlog = spec.server_obj.get_log()
                if tlog.get_term() == term:
                    done.append(spec)
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
            log_record_count = new_leader.server_obj.get_log().get_last_index()
            done = []
            for spec in [kept_1, kept_2, kept_3]:
                tlog = spec.server_obj.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(spec)
            if len(done) == 3:
                break
            time.sleep(0.1)
        msg=f"Log commits missing for {10 * self.timeout_basis} seconds"
        self.assertEqual(len(done), 3, msg)
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
        while time.time() - start_time <  10 * self.timeout_basis:
            done = []
            for spec in [leader, lost_1]:
                tlog = spec.server_obj.get_log()
                # log indices are 1 offset, not zero
                if tlog.get_commit_index() == log_record_count:
                    done.append(spec)
            
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
                      
