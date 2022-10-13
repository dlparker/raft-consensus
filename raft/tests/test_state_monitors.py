import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.tests.bt_server import MemoryBankTellerServer
from raft.tests.bt_client import MemoryBankTellerClient
from raft.states.state_map import StandardStateMap, StateChangeMonitor
from raft.tests.common_test_code import RunData, run_data_from_status
from raft.tests.setup_utils import Cluster

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

class Monitor1(StateChangeMonitor):

    def __init__(self, name, logger):
        self.name = name
        self.logger = logger
        self.state_history = []

    async def new_state(self, state_map, old_state, new_state):
        import threading
        this_id = threading.Thread.ident
        self.logger.info(f"{self.name} from {old_state} to {new_state}")
        if len(self.state_history) > 0:
            if str(new_state) == str(self.state_history[-1]):
                raise Exception("change is no change")
        self.state_history.append(new_state)
        return new_state
    
class TestMonitors(unittest.TestCase):

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
        self.cluster = Cluster(server_count=3,
                               use_processes=False,
                               logging_type=LOGGING_TYPE,
                               base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
                
    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.5)
        self.cluster.stop_logging_server()
        self.loop.close()
    
    def test_callabacks(self):
        self.cluster.prep_mem_servers()
        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            state_map = StandardStateMap()
            mserver.state_map = state_map
            monitor = Monitor1(name, self.logger)
            state_map.add_state_change_monitor(monitor)

        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            mserver.configure()
        self.cluster.start_all_servers()
        self.logger.info("waiting for election results")
        client = MemoryBankTellerClient("localhost", 5000)

        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            status = client.get_status()
            if status and status.data['leader']:
                leader_addr = status.data['leader']
                break
            status = None
            
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        run_data = run_data_from_status(self.cluster, self.logger, status)

        
