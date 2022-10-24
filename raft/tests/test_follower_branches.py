import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.termstart import TermStartMessage
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

    
class TestMessageOps(unittest.TestCase):

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
        
    def test_heartbeat_count(self):
        self.cluster.prep_mem_servers()
        monitors = []
        servers = []
        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            servers.append(mserver)
            mserver.configure()
            monitor = mserver.monitor
            monitor.set_pause_on_substate(Substate.joined)
            monitor.set_pause_on_substate(Substate.new_leader)
            monitor.set_pause_on_substate(Substate.became_leader)
            monitors.append(monitor)
        self.cluster.start_all_servers()
        
        self.logger.info("waiting for pause on election results")
        start_time = time.time()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            pause_count = 0
            for server in servers:
                if server.paused:
                    pause_count += 1
            if pause_count == 3:
                break
        self.assertEqual(pause_count, 3,
                         msg=f"only {pause_count} servers paused on election")

        async def resume():
            for server in servers:
                await server.resume_all()

        follower = None
        for monitor in monitors:
            if monitor.state._type == "follower":
                follower = monitor
                break
        inner_server = follower.pbt_server.thread.server
        initial_count = inner_server.state.heartbeat_count
        count = initial_count
        self.assertIsNotNone(follower)
        self.loop.run_until_complete(resume())
        count = inner_server.state.heartbeat_count
        start_time = time.time()
        while time.time() - start_time < 5 and count == initial_count:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            count = inner_server.state.heartbeat_count
        self.assertTrue(count > initial_count)


class TestStateEdges(unittest.TestCase):

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
        self.loop.close()
    
    def test_direct_termination(self):
        self.cluster.prep_mem_servers()
        name = list(self.cluster.server_recs.keys())[0]
        sdef = self.cluster.server_recs[name]
        
        mserver = sdef['memserver']
        mserver.configure()
        monitor = mserver.monitor
        monitor.set_pause_on_substate(Substate.leader_lost)
        self.cluster.start_one_server(name)
        self.logger.info("waiting for leader lost pause")
        start_time = time.time()
        while time.time() - start_time < 1.5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            if mserver.paused:
                break
        self.assertTrue(mserver.paused)
        # Now terminate it and resume, which should not result in an
        # election because of the terminated flag
        pre_state = monitor.state
        monitor.state.terminated = True
        async def resume():
            await mserver.resume_all()
        self.loop.run_until_complete(resume())
        time.sleep(0.25)
        # if election started, state will have changed to candidate
        self.assertEqual(pre_state, monitor.state)

        # Now try to force the election to start and ensure it does
        # not due to the terminated flag
        async def start_election():
            await monitor.state.start_election()
        self.loop.run_until_complete(start_election())
        self.assertEqual(pre_state, monitor.state)

        # Now make sure trying to start it raises
        with self.assertRaises(Exception) as context:
            monitor.state.start()
        self.assertTrue("terminated" in str(context.exception))

        async def do_stop():
            await monitor.state.stop()
            
        # Since already terminated, stop should no-op.
        # Since stop terminates the timer, no-op should leave it running
        self.loop.run_until_complete(do_stop())
        self.assertFalse(monitor.state.leaderless_timer.terminated)
        # Unset the termination flag and then call stop.
        # Should terminate the timer
        monitor.state.terminated = False
        self.loop.run_until_complete(do_stop())
        self.assertTrue(monitor.state.leaderless_timer.terminated)
        self.cluster.stop_server(name)
                    
        
        



        
            
