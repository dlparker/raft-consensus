import unittest
import asyncio
import time
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
        self.start_res = start_servers(base_port=5000)

    def tearDown(self):
        for name,sdef in self.start_res.items():
            stop_server(sdef)

    def test_one(self):
        logger = logging.getLogger()
        logger.info("starting test_one")
        client =  UDPBankTellerClient("localhost", 5000)
        start_time = time.time()
        while time.time() - start_time < 3:
            try:
                client.do_credit(10)
                break
            except:
                pass
        balance = client.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("calls to 5000 worked")
        # make sure that call to non-leader works
        client2 =  UDPBankTellerClient("localhost", 5001)
        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("call to 5001 worked")
        # now kill server one, at 5000, and make sure client2 still works
        for name,sdef in self.start_res.items():
            if sdef['port'] == 5000:
                stop_server(sdef)
                logger.info("server at 5000 stopped")
                break

        # might take a bit for failover
        start_time = time.time()
        while time.time() - start_time < 10:
            try:
                client2.do_credit(10)
                time.sleep(0.5)
                break
            except:
                pass
        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 20")
        logger.info("call to 5001 worked")

    def test_utils(self):
        # this just tests some utility functions that may not be called
        # otherwise, such as __str__ functions that are only used in
        # debug logging
        # get to BaseMessage through RequestVoteMessage
        from raft.messages.request_vote import RequestVoteMessage
        msg = RequestVoteMessage("sender", "target", 10, "data")
        self.assertTrue("sender" in str(msg))
        self.assertTrue("target" in str(msg))
        self.assertTrue("10" in str(msg))
        self.assertTrue("data" in str(msg))

        from raft.messages.serializer import Serializer
        # mess up the type of the vote message
        msg._type = "foo"
        bad_data = Serializer.serialize(msg)
        new_msg = Serializer.deserialize(bad_data)
        self.assertIsNone(new_msg)

        from raft.states.timer import Timer
        def my_interval():
            return 10
        t1 = Timer(my_interval, None)
        self.assertEqual(t1.get_interval(), 10)
        t2 = Timer(20, None)
        self.assertEqual(t2.get_interval(), 20)
