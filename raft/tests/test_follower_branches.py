import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path

from raft.states.base_state import Substate
from raft.dev_tools.ps_cluster import PausingServerCluster, PausePoint

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2
    
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
        
    def test_heartbeat_count(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.add_pause_point(PausePoint.election_done)
        self.cluster.start_all_servers()
        self.logger.info("waiting for election results")
        paused = self.cluster.wait_for_pause()
        self.assertTrue(paused)

        follower = None
        for spec in servers.values():
            if spec.monitor.state._type == "follower":
                follower = spec
                break
        inner_server = follower.server_obj
        initial_count = inner_server.state.heartbeat_count
        count = initial_count
        self.assertIsNotNone(follower)
        self.cluster.resume_from_stepper_pause()
        start_time = time.time()
        while time.time() - start_time < 5 and count == initial_count:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
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
        self.cluster = PausingServerCluster(server_count=3,
                                            logging_type=LOGGING_TYPE,
                                            base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
                
    def tearDown(self):
        self.loop.close()
    
    def test_direct_termination(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        name = "server_0"
        spec = servers[name]
        spec.monitor.set_pause_on_substate(Substate.leader_lost)
        self.cluster.start_one_server(name)
        self.logger.info("waiting for leader lost pause")
        start_time = time.time()
        while time.time() - start_time < 1.5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if spec.pbt_server.paused:
                break
        self.assertTrue(spec.pbt_server.paused)
        # Now terminate it and resume, which should not result in an
        # election because of the terminated flag
        pre_state = spec.monitor.state
        spec.monitor.state.terminated = True
        async def do_resume():
            await spec.pbt_server.resume_all()
        self.loop.run_until_complete(do_resume())
        time.sleep(0.25)
        # if election started, state will have changed to candidate
        self.assertEqual(pre_state, spec.monitor.state)

        # Now try to force the election to start and ensure it does
        # not due to the terminated flag
        async def start_election():
            await spec.monitor.state.start_election()
        self.loop.run_until_complete(start_election())
        self.assertEqual(pre_state, spec.monitor.state)

        # Now make sure trying to start it raises
        with self.assertRaises(Exception) as context:
            spec.monitor.state.start()
        self.assertTrue("terminated" in str(context.exception))

        async def do_stop():
            await spec.monitor.state.stop()
            
        # Unset the termination flag and then call stop.
        # Should terminate the timer
        spec.monitor.state.terminated = False
        self.loop.run_until_complete(do_stop())
        self.assertTrue(spec.monitor.state.leaderless_timer.terminated)
        self.cluster.stop_server(name)
                    
        
        



        
            
