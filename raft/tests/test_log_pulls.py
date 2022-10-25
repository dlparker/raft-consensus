import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.log.memory_log import MemoryLog
from raft.messages.log_pull import LogPullMessage, LogPullResponseMessage
from raft.tests.pausing_app import InterceptorMode
from raft.states.base_state import Substate
from raft.tests.ps_cluster import PausingServerCluster, PausePoint

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

# the timeing gets a bit tricky letting the paused log puller restart,
# sometimes it gets leader_lost if the timeout_basis it too low
timeout_basis = 0.5
    
class TestLogPulls(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        if self.logger is None:
            self.logger = logging.getLogger(__name__)
        self.cluster = PausingServerCluster(server_count=3,
                                            logging_type=LOGGING_TYPE,
                                            base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
                
    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.1)
        self.loop.close()

    def preamble(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.start_all_servers()
        # wait for election by waiting for heartbeat exchange
        for spec in servers.values():
            # follower after first heartbeat that requires no
            # additional sync up actions
            spec.monitor.set_pause_on_substate(Substate.synced)
            # leader after term start message
            spec.monitor.set_pause_on_substate(Substate.sent_heartbeat)
        
        self.cluster.start_all_servers()
        self.logger.info("waiting for pause on election results")
        start_time = time.time()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
            if pause_count == 3:
                break
        self.assertEqual(pause_count, 3,
                         msg=f"only {pause_count} servers paused on election")

        self.first_spec = None
        self.second_spec = None
        for spec in servers.values():
            # follower after first heartbeat that requires no
            # additional sync up actions
            spec.monitor.clear_pause_on_substate(Substate.synced)
            # leader after term start message
            spec.monitor.clear_pause_on_substate(Substate.sent_heartbeat)
            if spec.monitor.state._type == "leader":
                self.leader_spec = spec
            elif self.first_spec is None:
                self.first_spec = spec
            else:
                self.second_spec = spec
        self.assertIsNotNone(self.leader_spec)
        self.assertIsNotNone(self.first_spec)
        self.assertIsNotNone(self.second_spec)
        self.cluster.resume_all_paused_servers()
        self.client = self.leader_spec.get_client()
        start_time = time.time()
        while time.time() - start_time < 0.5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            status = self.client.get_status()
            if status:
                break
            status = None
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        # now stop one follower, do some log record creating
        # ops, the restart the follower and walk it through
        # the log pull
        self.stopper = self.second_spec.name
        self.cluster.stop_server(self.stopper)
        self.client.do_credit(10)
        self.client.do_credit(10)
        self.client.do_debit(5)
        self.client.do_debit(5)
        
        self.second_spec = self.cluster.prepare_one(self.stopper, restart=True,
                                 timeout_basis=timeout_basis)
        self.second_spec.monitor.set_pause_on_substate(Substate.joined)
        self.cluster.start_one_server(self.stopper)
        
        self.logger.info("waiting for restart complete to join")
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if self.second_spec.pbt_server.paused:
                break
        self.assertTrue(self.second_spec.pbt_server.paused)
        self.target_log = self.second_spec.server_obj.get_log()
        self.assertIsNone(self.target_log.read())

        # now the restarted server is paused at substate joined,
        # letting it go will cause log catch up.
        
    def test_normal_pull(self):
        self.preamble()
        # now resume so that the normal log pull sequence will take place
        async def resume():
            await self.second_spec.pbt_server.resume_all()
        self.loop.run_until_complete(resume())
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            rec = self.target_log.read()
            if rec and rec.index == 3:
                break
        
        self.assertIsNotNone(rec)
        self.assertEqual(rec.index, 3)

    def test_empty_leader_pull(self):
        self.preamble()
        # If we just resume the follower now, it will request 4
        # records from the leader. This will happen because the leader
        # sends a heartbeat to the follower that says the follower is
        # behind, so the follower sends a log pull request. If we
        # intercept the log pull request at the leader and empty
        # the log, then the leader should tell the follower to
        # reset, and then it should drop all the log records

        leader_inter = self.leader_spec.pbt_server.interceptor
        leader_inter.add_trigger(InterceptorMode.in_before,
                                 LogPullMessage._code)
        
        follow_inter = self.second_spec.pbt_server.interceptor
        follow_inter.add_trigger(InterceptorMode.in_after,
                                 LogPullResponseMessage._code)
        
        async def resume_follower():
            await self.second_spec.pbt_server.resume_all()
        self.loop.run_until_complete(resume_follower())
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            rec = self.target_log.read()
            if self.leader_spec.pbt_server.paused:
                break

        self.assertTrue(self.leader_spec.pbt_server.paused)
        # just replace the log with an empty one, but don't
        # break the term
        old_term  = self.leader_spec.server_obj.log.get_term()
        leader_log = self.leader_spec.server_obj.log = MemoryLog()
        leader_log.set_term(old_term)
        
        
        async def resume_leader():
            await self.leader_spec.pbt_server.resume_all()
        self.loop.run_until_complete(resume_leader())

        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            if self.second_spec.pbt_server.paused:
                break
        self.assertTrue(self.second_spec.pbt_server.paused)
        self.loop.run_until_complete(resume_follower())
        self.assertIsNone(self.target_log.read())

    def test_short_leader_pull(self):
        self.preamble()
        # If we just resume the follower now, it will request 4
        # records from the leader. This will happen because the leader
        # sends a heartbeat to the follower that says the follower is
        # behind, so the follower sends a log pull request.
        # Instead,
        # Copy three of the leader log records to the follower.
        # Let the follower run to cause a pull request, but this
        # time it will say that the start index is 2, so it
        # is asking for pull from index 2 to the end.
        # Pause the leader before handling the pull request and
        # remove the last two records. That means that the
        # leader will send a "reset" message back telling the follower
        # to trim the log back to index 1.
        # This is an oddball scenario, but the code under
        # test is written as though it is possible. It may
        # be only excessively defensive programming, but since
        # it is there, it should be tested.

        # copy the first four messages to the follower's log
        leader_log = self.leader_spec.server_obj.log
        recs = []
        for i in range(3):
            recs.append(leader_log.read(i))            
        self.target_log.append(recs)

        # stop the follower after it sends the pull message, timeouts
        # are short in test mode so it might hit one
        follow_inter = self.second_spec.pbt_server.interceptor
        follow_inter.add_trigger(InterceptorMode.out_after,
                                 LogPullMessage._code)
        
        # stop the leader before it handles the pull message
        leader_inter = self.leader_spec.pbt_server.interceptor
        leader_inter.add_trigger(InterceptorMode.in_before,
                                 LogPullMessage._code)
        
        async def resume_follower():
            await self.second_spec.pbt_server.resume_all()
        self.loop.run_until_complete(resume_follower())

        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if self.leader_spec.pbt_server.paused:
                break
        self.assertTrue(self.leader_spec.pbt_server.paused)
        # Trim the leader log to just two entries, so that the
        # leader will think that the follower has asked for
        # records with a start index above the last index
        leader_log.trim_after(1)

        # clear current pause
        follow_inter.clear_trigger(InterceptorMode.out_after,
                                 LogPullMessage._code)
        # stop the follower after it handles the pull message response
        follow_inter.add_trigger(InterceptorMode.in_after,
                                 LogPullResponseMessage._code)
        
        async def resume_leader():
            await self.leader_spec.pbt_server.resume_all()
        # resume the leader
        self.loop.run_until_complete(resume_leader())
        # resume the follower
        self.loop.run_until_complete(resume_follower())

        # wait for the follower to hit the new interceptor pause
        # on the in log pull response
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if self.second_spec.pbt_server.paused:
                break
        self.assertTrue(self.second_spec.pbt_server.paused)

        # now a resume should have it clearing log data until
        # it matches leader
        self.loop.run_until_complete(resume_follower())
        rec = self.target_log.read()
        self.assertIsNotNone(rec)
        # if all went well, the follower will now have just two
        # records in the log
        self.assertEqual(rec.index, 1)


        







        
            
