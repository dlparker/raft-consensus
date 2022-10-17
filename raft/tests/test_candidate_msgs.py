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

    def test_candidate_gets_query(self):
        self.cluster.prep_mem_servers()
        servers = []
        monitors = []

        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            servers.append(mserver)
            mserver.configure()
            monitor = mserver.monitor
            monitor.set_pause_on_substate(Substate.voting)
            monitors.append(monitor)

        # start just the one server and wait for it
        # to pause in candidate state
        self.cluster.start_one_server(servers[0].name)
        self.logger.info("waiting for pause election start")
        start_time = time.time()
        monitor = monitors[0]
        while time.time() - start_time < 3:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.05)
            if monitor.state is not None:
                if str(monitor.state) == "candidate":
                    if monitor.substate == Substate.voting:
                        break

        self.assertEqual(monitor.substate, Substate.voting)
        servers[0].comms.resume()
        client =  MemoryBankTellerClient("localhost", 5000)
        status = client.get_status()
        res = client.do_credit(10)
        self.assertTrue('not available' in res)
        servers[0].resume_all()
        







        
            
