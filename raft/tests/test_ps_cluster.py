import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.dev_tools.bt_client import MemoryBankTellerClient
from raft.dev_tools.ps_cluster import PausingServerCluster, PausePoint

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2
    
class TestPausing(unittest.TestCase):

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
    
    def test_trivial(self):
        # just make sure that we can run servers using cluster code
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.start_all_servers()
        self.logger.info("waiting for election results")
        client = MemoryBankTellerClient("localhost", 5000)
        start_time = time.time()
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.05)
            status = client.get_status()
            if status and status.data['leader']:
                leader_addr = status.data['leader']
                break
            status = None
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])

    def test_election_pause(self):
        # Test the pause support for just after election is done
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.add_pause_point(PausePoint.election_done)
        self.cluster.start_all_servers()
        self.logger.info("waiting for election results")
        paused = self.cluster.wait_for_pause()
        self.assertTrue(paused)
        self.cluster.resume_from_stepper_pause()

        # make sure it works after resume
        client = MemoryBankTellerClient("localhost", 5000)
        status = client.get_status()
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        
    def test_log_pull_pause(self):
        # Test the pause support for a follower requesting
        # a log pull in order to catch up with leader. We first
        # need to start two of the three servers and let them
        # complete the election. We don't technically have to use
        # the pause method of verifying that the election happened,
        # but it is easy to do.
        # After that, we put some items in the log by doing client
        # ops. Then we set pause for the log_pull messages and
        # start the third server. It should notice that is behind
        # the leader and ask for a log pull, which should then
        # pause the third server and the leader, but not the
        # original follower.
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.add_pause_point(PausePoint.election_done)
        started = 0
        for name, spec in self.cluster.get_servers().items():
            self.cluster.start_one_server(name)
            started += 1
            if started == 2:
                break
        self.logger.info("waiting for election results")
        paused = self.cluster.wait_for_pause()
        self.assertTrue(paused)
        self.cluster.resume_and_add_pause_point(PausePoint.log_pull_straddle)

        client = MemoryBankTellerClient("localhost", 5000)
        # make some log records
        client.do_credit(10)
        client.do_credit(10)
        client.do_debit(5)
        # now start third server and look for 2 to pause
        for name, spec in self.cluster.get_servers().items():
            if spec.running:
                continue
            self.cluster.start_one_server(name)
        paused = self.cluster.wait_for_pause(expected_count=2)
        self.cluster.resume_from_stepper_pause()




        
            
