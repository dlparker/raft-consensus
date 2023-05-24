import asyncio
import time
from pathlib import Path
import os

from tests.common_tcase import TestCaseCommon
from raftframe.log.sqlite_log import SqliteLog
from raftframe.states.base_state import Substate

class TestRestarts(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestRestarts, cls).setUpClass()
        cls.total_nodes = 3
        # running too slow causes problems with
        # candidates timeout or pushing leader into
        # problem state regarding our pausing tricks
        cls.timeout_basis = 0.1
        if __name__.startswith("tests"):
            cls.logger_name = __name__
        else:
            cls.logger_name = "tests." + __name__
        cls.log_paths = {}

    def change_log(self):
        for pserver in self.cluster.servers:
            path = Path(f"/tmp/raft_tests/{pserver.name}")
            if not path.exists():
                path.mkdir(parents=True)
            filepath = Path(path, "log.sqlite")
            if filepath.exists():
                filepath.unlink()
            if not pserver.name in self.log_paths:
                self.log_paths[pserver.name] = filepath
            pserver.data_log = SqliteLog(path)

                      
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
        
    def restarts_inner(self, use_sqlite=False):
        if use_sqlite:
            self.preamble(pre_start_callback=self.change_log)
        else:
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
        

        # clear the role references
        orig_leader = self.leader
        self.leader = None
        self.logger.debug("\n\n\tStoping Leader %s \n\n", orig_leader.name)
        self.expected_followers = 1
        self.clear_pause_triggers()
        for pserver in self.cluster.servers:
            # pause leader after new term record
            pserver.pause_on_substate(Substate.became_leader)
            # pause followers after they accept leader
            pserver.pause_on_substate(Substate.joined)
            
        orig_leader.stop()
        start_time = time.time()
        while time.time() - start_time < 3 * self.timeout_basis:
            if not orig_leader.running:
                break
        self.assertFalse(orig_leader.running)
        # just make sure election happens so we know who's who
        self.wait_till_paused(2)
        for pserver in self.cluster.servers:
            if not pserver.running:
                continue
            if str(pserver.state_map.state) == 'leader':
                self.leader = pserver
            else:
                still_following = pserver
        second_leader = self.leader
        self.clear_pause_triggers()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tQuery to %s \n\n", second_leader.name)
        client2 = self.leader.get_client()
        res2 = client2.do_query()
        self.assertEqual(res2['balance'], 10)

        time.sleep(0.01) # allow time for the commit messages to flow
        
        self.logger.debug("\n\n\tRestarting original leader %s \n\n",
                          orig_leader.name)
        
        restarted = self.cluster.regen_server(orig_leader)
        restarted.start()
        time.sleep(0.1)
        # wait for restarted server to catch up
        trec = None
        start_time = time.time()
        while time.time() - start_time < 4 * self.timeout_basis:
            log_record_count = self.leader.thread.server.get_log().get_last_index()
            if restarted.thread.server:
                tlog = restarted.thread.server.get_log()
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
        # logic that enforces leader only ops. Not actually tested, because
        # we wouldn't know if somebody changed the code and allowed the
        # follower to answer directly, so more "exercised" than "tested"
        client3 = restarted.get_client()
        res3 = client3.do_query()
        log_record_count += 1
        self.assertEqual(res3['balance'], 10)
        
        # kill and restart a follower and wait for it to have an up to date log
        target = still_following
        
        self.logger.debug("\n\n\tStopping follower %s \n\n", target.name)
        target.stop()
        self.logger.debug("\n\n\tRestarting follower %s \n\n", target.name)
        
        new_target = self.cluster.regen_server(target)
        new_target.start()
        start_time = time.time()
        trec = None
        while time.time() - start_time < 4:
            if new_target.thread.server:
                tlog = new_target.thread.server.get_log()
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
