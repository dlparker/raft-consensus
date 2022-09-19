import unittest
import asyncio
import logging

from raft.tests.setup_utils import start_servers, stop_server
from raft.tests.bt_client import UDPBankTellerClient


class TestTwoWay(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):
        logging.getLogger().debug("starting test_one")
        start_res = start_servers(base_port=5000)
        import time
        time.sleep(1)
        client =  UDPBankTellerClient("localhost", 5000)
        client.do_credit(10)
        balance = client.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        for name,sdef in start_res.items():
            stop_server(sdef)
        
