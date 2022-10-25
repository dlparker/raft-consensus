import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.termstart import TermStartMessage
from raft.dev_tools.bt_client import MemoryBankTellerClient
from raft.states.base_state import Substate
from raft.dev_tools.ps_cluster import PausingServerCluster, PausePoint


LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2
    
class TestDelayedStart(unittest.TestCase):

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

    def test_start_one_after_election(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.add_pause_point(PausePoint.election_done)
        self.cluster.start_one_server("server_0")
        self.cluster.start_one_server("server_1")
        self.logger.info("waiting for pause on election results")
        paused = self.cluster.wait_for_pause(timeout=2, expected_count=2)
        self.assertTrue(paused)
        self.cluster.resume_from_stepper_pause()
        self.cluster.start_one_server("server_2")
        spec2 = servers['server_2']
        self.logger.info("waiting third server start")
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.05)
            if spec2.monitor.state is not None:
                break
        self.assertEqual(spec2.monitor.state._type, "follower")







        
            
