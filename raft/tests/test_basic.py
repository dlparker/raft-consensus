import unittest
import asyncio
import time
import logging

from raft.tests.setup_utils import start_servers, stop_server
from raft.tests.bt_client import UDPBankTellerClient


class TestUtils(unittest.TestCase):

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
        
class TestFourServers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.start_res = start_servers(base_port=5000, num_servers=4)

    def tearDown(self):
        for name,sdef in self.start_res.items():
            stop_server(sdef)

    def test_non_leader_stop(self):
        logger = logging.getLogger()
        logger.info("starting test_non_leader_stop")
        client1 =  UDPBankTellerClient("localhost", 5000)
        start_time = time.time()
        while time.time() - start_time < 3:
            try:
                client1.do_credit(10)
                break
            except:
                time.sleep(0.5)
        status = client1.get_status()
        leader_addr = status.data['leader']
        leader = None
        first_follower = None
        second_follower = None
        for name,sdef in self.start_res.items():
            if sdef['port'] == leader_addr[1]:
                sdef['role'] = "leader"
                leader = sdef
            else:
                if not first_follower:
                    first_follower = sdef
                else:
                    second_follower = sdef
                sdef['role'] = "follower"

        balance = client1.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("calls to 5000 worked")
        # get a client for the first follower
        client2 =  UDPBankTellerClient("localhost",
                                       first_follower['port'])
        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("call to 5001 worked")

        logger.info("stopping non-leader server %s", second_follower)
        stop_server(second_follower)
        logger.info("server %s stopped", second_follower)
            
        # make sure that calls to both running servers work
        balance = client1.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        client2.do_credit(10)
        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 20")
        logger.info("all operations working with one non-leader server down")

    def test_leader_stop(self):
        logger = logging.getLogger()
        logger.info("starting test_leader_stop")
        client1 =  UDPBankTellerClient("localhost", 5000)
        start_time = time.time()
        while time.time() - start_time < 3:
            try:
                client1.do_credit(10)
                break
            except:
                time.sleep(0.5)
        status = client1.get_status()
        leader_addr = status.data['leader']
        leader = None
        first_follower = None
        second_follower = None
        for name,sdef in self.start_res.items():
            if sdef['port'] == leader_addr[1]:
                sdef['role'] = "leader"
                leader = sdef
            else:
                if not first_follower:
                    first_follower = sdef
                else:
                    second_follower = sdef
                sdef['role'] = "follower"

        balance = client1.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("calls to 5000 worked")
        # get a client for the first follower
        client2 =  UDPBankTellerClient("localhost",
                                       first_follower['port'])
        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("call to 5001 worked")

        logger.info("stopping leader server %s", leader)
        stop_server(leader)
        logger.info("server %s stopped", leader)

        if leader['port'] == 5000:
            new_client = client2
        else:
            new_client = client1
        # wait a second for election to happen
        time.sleep(1)
        start_time = time.time()
        while time.time() - start_time < 7:
            try:
                status = new_client.get_status()
                new_leader_addr = status.data['leader']
                if (new_leader_addr[0] != -1 &
                    new_leader_addr[1] != leader['port']):
                    break
            except:
                time.sleep(0.5)
        self.assertNotEqual(new_leader_addr[0], -1,
                            msg="Leader election started but did not complete")
        self.assertNotEqual(new_leader_addr[1], leader['port'],
                            msg="Leader election never happend")
        balance = new_client.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        new_client.do_credit(10)
        balance = new_client.do_query()
        self.assertEqual(balance, "Your current account balance is: 20")
        logger.info("all operations working after election")


