import unittest
import asyncio
import time
import logging

from raft.tests.setup_utils import start_servers, stop_server
from raft.tests.log_control import setup_logging_for_test, stop_logging_server
from raft.tests.bt_client import UDPBankTellerClient
from raft.states.log_api import LogRec
from raft.states.memory_log import MemoryLog


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


class TestMemoryLog(unittest.TestCase):

    def test_mem_log(self):
        mlog = MemoryLog()
        empty_tail = mlog.get_tail()
        self.assertEqual(empty_tail.last_index, -1)
        self.assertEqual(empty_tail.term, None)
        self.assertEqual(empty_tail.commit_index, -1)
        rec1_data = dict(name="rec1", value=1)
        rec1 = LogRec(user_data=rec1_data)
        mlog.append([rec1,], 1)
        one_rec_tail = mlog.get_tail()
        self.assertEqual(one_rec_tail.last_index, 0)
        self.assertEqual(one_rec_tail.term, 1)
        self.assertEqual(one_rec_tail.commit_index, -1)
        mlog.commit()
        commit_tail = mlog.get_tail()
        self.assertEqual(commit_tail.commit_index, 0)
        rec2_data = dict(name="rec2", value=2)
        rec2 = LogRec(user_data=rec2_data)
        rec3_data = dict(name="rec3", value=3)
        rec3 = LogRec(user_data=rec3_data)
        mlog.append([rec2, rec3], 2)
        mlog.commit()
        three_rec_tail = mlog.get_tail()
        self.assertEqual(three_rec_tail.last_index, 2)
        self.assertEqual(three_rec_tail.term, 2)
        self.assertEqual(three_rec_tail.commit_index, 2)

        rec1_read = mlog.read(0)
        self.assertEqual(rec1_read.user_data, rec1_data)
        self.assertEqual(rec1_read.index, 0)
        self.assertEqual(rec1_read.term, 1)
        rec2_read = mlog.read(1)
        self.assertEqual(rec2_read.user_data, rec2_data)
        self.assertEqual(rec2_read.index, 1)
        self.assertEqual(rec2_read.term, 2)
        rec3_read = mlog.read(2)
        self.assertEqual(rec3_read.user_data, rec3_data)
        self.assertEqual(rec3_read.index, 2)
        self.assertEqual(rec3_read.term, 2)

        no_rec = mlog.read(3)
        self.assertIsNone(no_rec)

        mlog.trim_after(0)
        trimmed_tail = mlog.get_tail()
        self.assertEqual(trimmed_tail.last_index, 0)
        self.assertEqual(trimmed_tail.term, 1)
        self.assertEqual(trimmed_tail.commit_index, 0)
        
        
        
class TestThreeServers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.log_config = setup_logging_for_test("TestTreeServersCase")

    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.start_res = start_servers(base_port=5000, num_servers=3,
                                       log_config=self.log_config)

    def tearDown(self):
        for name,sdef in self.start_res.items():
            stop_server(sdef)
        time.sleep(0.5)
        stop_logging_server()

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
                status = client1.get_status()
                break
            except:
                time.sleep(0.5)
                status = None
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
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

        client1.do_credit(10)
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


