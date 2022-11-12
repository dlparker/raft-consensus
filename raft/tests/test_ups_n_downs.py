import asyncio
import time
from pathlib import Path
import os

from raft.tests.common_tcase import TestCaseCommon
from raft.log.sqlite_log import SqliteLog

class TestRestarts(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestRestarts, cls).setUpClass()
        cls.total_nodes = 3
        # running too slow causes problems with
        # candidates timeout or pushing leader into
        # problem state regarding our pausing tricks
        cls.timeout_basis = 0.2
        if __name__.startswith("raft.tests"):
            cls.logger_name = __name__
        else:
            cls.logger_name = "raft.tests." + __name__
        cls.log_paths = {}

    def change_log(self):
        for spec in self.cluster.get_servers().values():
            path = Path(f"/tmp/raft_tests/{spec.name}")
            filepath = Path(path, "log.sqlite3")
            if not spec.name in self.log_paths:
                self.log_paths[spec.name] = filepath
            spec.pbt_server.data_log = SqliteLog(path)

    def restarts_inner(self, use_sqlite=False):
        if use_sqlite:
            self.timeout_basis = 1.0
            self.cluster.timeout_basis = 1.0
            self.preamble(pre_start_callback=self.change_log)
        else:
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
        

        # clear the role references
        orig_leader = self.leader
        self.leader = None
        self.logger.debug("\n\n\tStoping Leader %s \n\n", orig_leader.name)
        self.expected_followers = 1
        self.reset_pausers()
        self.set_term_start_intercept()
        self.cluster.stop_server(orig_leader.name)
        self.assertFalse(orig_leader.running)
        # just make sure election happens so we know who's who
        self.pause_waiter("waiting for pause leader lost new election",
                          expected=2)
        second_leader = self.leader
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tQuery to %s \n\n", second_leader.name)
        client2 = self.leader.get_client()
        res2 = client2.do_query()
        self.assertEqual(res2['balance'], 10)

        time.sleep(0.01) # allow time for the commit messages to flow
        
        self.logger.debug("\n\n\tRestarting original leader %s \n\n",
                          orig_leader.name)
        restarted = self.cluster.prepare_one(orig_leader.name)
        self.update_pauser(restarted)
        self.cluster.start_one_server(restarted.name)
        self.logger.debug("\n\n\tActivating heartbeat intercepts \n\n")
        # wait for restarted server to catch up
        start_time = time.time()
        trec = None
        while time.time() - start_time < 4:
            log_record_count = self.leader.server_obj.get_log().get_last_index()
            tserver = restarted.server_obj
            if tserver:
                tlog = tserver.get_log()
                trec = tlog.read()
                if trec:
                    if trec.index == log_record_count:
                        break
            time.sleep(0.01)
        self.assertIsNotNone(trec)
        # log indicies are one based, not zero
        self.assertEqual(trec.index, log_record_count)
        self.logger.debug("\n\n\tRestarted %s log catch up done \n\n",
                          restarted.name)
        # now make sure that a call to the restarted leader, now follower
        # results in the correct results, thereby testing the forwarding
        # logic that enforces leader only ops.
        client3 = restarted.get_client()
        res3 = client3.do_query()
        log_record_count += 1
        self.assertEqual(res3['balance'], 10)
        
        # kill and restart a follower and wait for it to have an up to date log
        target = self.non_leaders[0]
        
        self.leader = None
        self.logger.debug("\n\n\tStopping follower %s \n\n", target.name)
        self.cluster.stop_server(target.name)
        self.logger.debug("\n\n\tRestarting follower %s \n\n", target.name)
        new_target = self.cluster.prepare_one(target.name)
        self.cluster.start_one_server(new_target.name)
        start_time = time.time()
        trec = None
        while time.time() - start_time < 4:
            tserver = new_target.server_obj
            if tserver:
                tlog = tserver.get_log()
                trec = tlog.read()
                if trec:
                    if trec.index == log_record_count:
                        break
            time.sleep(0.01)
        self.assertIsNotNone(trec)
        # log indicies are one based, not zero
        self.assertEqual(trec.index, log_record_count)
        self.logger.debug("\n\n\tRestarted follower %s log catch up done \n\n",
                          new_target.name)

        self.logger.debug("\n\n\tDone with test, starting shutdown\n")
        # check the actual log in the follower
        self.postamble()
                      
    def test_restarts(self):
        self.restarts_inner()

    def test_restarts_with_sqlite(self):
        self.restarts_inner(True)
