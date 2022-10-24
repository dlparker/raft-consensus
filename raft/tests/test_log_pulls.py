import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.termstart import TermStartMessage
from raft.log.memory_log import MemoryLog
from raft.messages.log_pull import LogPullMessage, LogPullResponseMessage
from raft.tests.bt_client import MemoryBankTellerClient
from raft.tests.pausing_app import InterceptorMode, TriggerType
from raft.states.base_state import Substate
from raft.tests.common_test_code import run_data_from_status
from raft.tests.setup_utils import Cluster
from raft.tests.timer import get_timer_set
from raft.comms.memory_comms import reset_queues

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

    
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
        reset_queues()
        self.cluster = Cluster(server_count=3,
                               use_processes=False,
                               logging_type=LOGGING_TYPE,
                               base_port=5000,
                               use_pauser=True)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
                
    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.1)
        self.cluster.stop_logging_server()
        self.loop.close()

    def preamble(self):
        # just make sure that all the monitors get called
        self.cluster.prep_mem_servers()
        monitors = []
        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            mserver.configure()
            monitor = mserver.monitor
            monitors.append(monitor)

        self.cluster.start_all_servers()
        self.client = MemoryBankTellerClient("localhost", 5000)
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            status = self.client.get_status()
            if status and status.data['leader']:
                leader_addr = status.data['leader']
                break
            status = None
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        self.leader_monitor = None
        first_mon = None
        second_mon = None
        def do_pause():
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(get_timer_set().pause_all())
            #do_pause()
        for monitor in monitors:
            if monitor.state is None:
                continue
            if str(monitor.state) == "leader":
                self.leader_monitor = monitor
            elif first_mon is None:
                first_mon = monitor
            else:
                second_mon = monitor
        self.assertIsNotNone(self.leader_monitor)
        self.assertIsNotNone(first_mon)
        self.assertIsNotNone(second_mon)
        run_data = run_data_from_status(self.cluster, self.logger, status)
        leader = run_data.leader
        first = run_data.first_follower
        second = run_data.second_follower
        addr = self.leader_monitor.state_map.server.endpoint
        self.assertEqual(addr, leader['addr'])
        leader['monitor'] = self.leader_monitor
        addr = first_mon.state_map.server.endpoint
        if addr == first['addr']:
            first['monitor'] = first_mon
            second['monitor'] = second_mon
        else:
            first['monitor'] = second_mon
            second['monitor'] = first_mon
            second_mon = first_mon
            first_mon = first['monitor']

        # now stop one follower, do some log record creating
        # ops, the restart the follower and walk it through
        # the log pull
        self.stopper = second['monitor'].pbt_server.name
        self.cluster.stop_server(self.stopper)
        self.client.do_credit(10)
        self.client.do_credit(10)
        self.client.do_debit(5)
        self.client.do_debit(5)
        
        sdef = self.cluster.prep_mem_server(self.stopper)
        mserver = sdef['memserver']
        mserver.configure()
        self.restarted_monitor = mserver.monitor
        self.restarted_monitor.set_pause_on_substate(Substate.joined)
        self.cluster.start_one_server(self.stopper)
        
        self.logger.info("waiting for restart complete to join")
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            if self.restarted_monitor.pbt_server.paused:
                break

        self.assertTrue(self.restarted_monitor.pbt_server.paused)
        self.target_log = self.restarted_monitor.pbt_server.thread.server.get_log()
        self.assertIsNone(self.target_log.read())
        
    def test_normal_pull(self):
        self.preamble()
        # now resume so that the normal log pull sequence will take place
        async def resume():
            await self.restarted_monitor.pbt_server.resume_all()
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

        leader_inter = self.leader_monitor.pbt_server.interceptor
        leader_inter.add_trigger(InterceptorMode.in_before,
                          LogPullMessage._code)
        
        follow_inter = self.restarted_monitor.pbt_server.interceptor
        follow_inter.add_trigger(InterceptorMode.in_after,
                                 LogPullResponseMessage._code)
        
        async def resume_follower():
            await self.restarted_monitor.pbt_server.resume_all()
        self.loop.run_until_complete(resume_follower())
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            rec = self.target_log.read()
            if self.leader_monitor.pbt_server.paused:
                break

        self.assertTrue(self.leader_monitor.pbt_server.paused)
        # just replace the log with an empty one, but don't
        # break the term
        old_term  = self.leader_monitor.pbt_server.thread.server.log.get_term()
        leader_log = self.leader_monitor.pbt_server.thread.server.log = MemoryLog()
        leader_log.set_term(old_term)
        
        
        async def resume_leader():
            await self.leader_monitor.pbt_server.resume_all()
        self.loop.run_until_complete(resume_leader())

        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            if self.restarted_monitor.pbt_server.paused:
                break
        self.assertTrue(self.restarted_monitor.pbt_server.paused)
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
        leader_log = self.leader_monitor.pbt_server.thread.server.log
        recs = []
        for i in range(3):
            recs.append(leader_log.read(i))            
        self.target_log.append(recs)

        # stop the leader before it handles the pull message
        leader_inter = self.leader_monitor.pbt_server.interceptor
        leader_inter.add_trigger(InterceptorMode.in_before,
                                 LogPullMessage._code)
        
        # stop the follower after it handles the pull message response
        follow_inter = self.restarted_monitor.pbt_server.interceptor
        follow_inter.add_trigger(InterceptorMode.in_after,
                                 LogPullResponseMessage._code)
        
        async def resume_follower():
            await self.restarted_monitor.pbt_server.resume_all()
        self.loop.run_until_complete(resume_follower())

        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            if self.leader_monitor.pbt_server.paused:
                break
        self.assertTrue(self.leader_monitor.pbt_server.paused)
        # Trim the leader log to just two entries, so that the
        # leader will think that the follower has asked for
        # records with a start index above the last index
        leader_log.trim_after(1)
        async def resume_leader():
            await self.leader_monitor.pbt_server.resume_all()
        self.loop.run_until_complete(resume_leader())

        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            if self.restarted_monitor.pbt_server.paused:
                break
        self.assertTrue(self.restarted_monitor.pbt_server.paused)
        self.loop.run_until_complete(resume_follower())
        rec = self.target_log.read()
        self.assertIsNotNone(rec)
        # if all went well, the follower will now have just two
        # records in the log
        self.assertEqual(rec.index, 1)


        







        
            
