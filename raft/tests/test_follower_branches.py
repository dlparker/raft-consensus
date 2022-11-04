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
        initial_count = inner_server.state_map.state.heartbeat_count
        count = initial_count
        self.assertIsNotNone(follower)
        self.cluster.resume_from_stepper_pause()
        start_time = time.time()
        while time.time() - start_time < 5 and count == initial_count:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            count = inner_server.state_map.state.heartbeat_count
        self.assertTrue(count > initial_count)


                    
        
        

        
            
