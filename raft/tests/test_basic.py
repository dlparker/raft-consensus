import unittest
import asyncio
import time
import logging
import traceback
import os

from raft.tests.timer import get_timer_set
from raft.tests.setup_utils import Cluster
from raft.tests.bt_client import UDPBankTellerClient, MemoryBankTellerClient
from raft.states.log_api import LogRec
from raft.states.memory_log import MemoryLog
from raft.states.follower import Follower
from raft.messages.regy import get_message_registry

#LOGGING_TYPE = "devel_one_proc" when using Mem comms and thread based servers
#LOGGING_TYPE = "devel_mp" when using UDP comms and MP process based servers
#LOGGING_TYPE = "silent" for no log at all
LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")

if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc" 
    

class TestUtils(unittest.TestCase):

    def test_messages(self):

        from raft.messages.base_message import BaseMessage
        from raft.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
        b_msg = BaseMessage('1', '2', 0, "{'x':1}", '3')
        str_version = str(b_msg)
        dict_version = b_msg.props_as_dict()
        for key,value in dict_version.items():
            if key in ['data', 'original_sender']:
                continue
            self.assertTrue(str(value) in str_version)
        # these two have the data in the string rep
        rv_msg = RequestVoteMessage('1', '2', 0, "{'x':1}")
        rvr_msg = RequestVoteResponseMessage('1', '2', 0, "{'x':1}")
        for t_msg in [rv_msg, rvr_msg]:
            str_version = str(t_msg)
            dict_version = t_msg.props_as_dict()
            for key,value in dict_version.items():
                if key in ['original_sender']:
                    continue
                self.assertTrue(str(value) in str_version)
        
        regy = get_message_registry()
        hb_c = regy.get_message_class("heartbeat")
        self.assertIsNotNone(hb_c)
        msg = hb_c('1', '2', 0, "{'x':1}")
        fo = Follower()
        mh = regy.get_handler(msg, fo)
        self.assertIsNotNone(mh)
        expected_codes = ['heartbeat', 'heartbeat_response', 
                 'status_query', 'status_query_response', 
                 ]
        
        codes = regy.get_message_codes()
        for code in expected_codes:
            self.assertTrue(code in codes)
        
        sqr_c = regy.get_message_class("status_query_response")
        self.assertIsNotNone(sqr_c)
        msg = sqr_c('1', '2', 0, "{'x':1}")
        sqr_h = regy.get_handler(msg, fo)
        self.assertIsNone(sqr_h)
        all_classes = regy.get_message_classes()
        from raft.messages.heartbeat import HeartbeatMessage
        self.assertTrue(HeartbeatMessage in all_classes)
        from raft.messages.heartbeat import HeartbeatResponseMessage
        # this should be legal, a re-register
        regy.register_message_class(HeartbeatMessage, "on_heartbeat_response")
        # this should not, conflicting values
        class Dummy(BaseMessage):
            _code = "heartbeat"
            def __init__(self, sender, receiver, term, data):
                BaseMessage.__init__(self, sender, receiver, term, data)
        with self.assertRaises(Exception) as context:
            regy.register_message_class(Dummy, "on_heartbeat")

        
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
        msg._code = "foo"
        bad_data = Serializer.serialize(msg)
        with self.assertRaises(Exception) as context:
            new_msg = Serializer.deserialize(bad_data)


        from raft.states.timer import Timer
        def my_interval():
            return 10
        async def runner():
            t1 = Timer(my_interval, None)
            self.assertEqual(t1.get_interval(), 10)
            t2 = Timer(20, None)
            self.assertEqual(t2.get_interval(), 20)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(runner())
            

class TestMemoryLog(unittest.TestCase):

    def test_mem_log(self):
        pass        
        
class TestThreeServers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = Cluster(server_count=3, use_processes=False,
                               logging_type=LOGGING_TYPE, base_port=5000)
        self.cluster.start_all_servers()

    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.5)
        self.cluster.stop_logging_server()

    def test_non_leader_stop(self):
        logger = logging.getLogger(__name__)
        logger.info("starting test_non_leader_stop")
        async def do_wait(seconds):
            start_time = time.time()
            while time.time() - start_time < seconds:
                await asyncio.sleep(0.01)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        client1 =  MemoryBankTellerClient("localhost", 5000)
        start_time = time.time()
        status = None
        status_exc = None
        while time.time() - start_time < 4:
            loop.run_until_complete(do_wait(0.25))
            try:
                status = client1.get_status()
                if status and status.data['leader']:
                    break
            except Exception as e:
                status_exc = e
                
        if status_exc:
            logger.error("last status call got %s",  traceback.format_exc(status_exc))
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        timer_set = get_timer_set()
        timer_set.pause_all()
        leader_addr = status.data['leader']
        leader = None
        first_follower = None
        second_follower = None
        for name,sdef in self.cluster.server_recs.items():
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
        client2 =  MemoryBankTellerClient("localhost", first_follower['port'])

        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("call to 5001 worked")

        status = client1.get_status()
        logger.info("stopping non-leader server %s", second_follower)
        self.cluster.stop_server(second_follower['name'])
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
        logger = logging.getLogger(__name__)
        logger.info("starting test_leader_stop")
        client1 =  UDPBankTellerClient("localhost", 5000)
        client1 =  MemoryBankTellerClient("localhost", 5000)
        status = None
        logger.info("waiting for election results")
        start_time = time.time()
        while time.time() - start_time < 3:
            time.sleep(0.25)
            status = client1.get_status()
            if status and status.data['leader']:
                break
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        leader_addr = status.data['leader']
        leader = None
        first_follower = None
        second_follower = None
        for name,sdef in self.cluster.server_recs.items():
            if sdef['port'] == leader_addr[1]:
                sdef['role'] = "leader"
                leader = sdef
            else:
                if not first_follower:
                    first_follower = sdef
                else:
                    second_follower = sdef
                sdef['role'] = "follower"
        logger.info("found leader %s", leader_addr)

        client1.do_credit(10)
        balance = client1.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("initial callse to 5000 worked")
        # get a client for the first follower
        client2 =  MemoryBankTellerClient("localhost",
                                       first_follower['port'])
        balance = client2.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        logger.info("call to 5001 worked")

        logger.info("stopping leader server %s", leader)
        self.cluster.stop_server(leader['name']) 
        logger.info("server %s stopped", leader)

        if leader['port'] == 5000:
            new_client = client2
        else:
            new_client = client1
        # wait for election to happen
        logger.info("waiting for election results")
        start_time = time.time()
        while time.time() - start_time < 7:
            time.sleep(0.25)
            status = new_client.get_status()
            if status:
                new_leader_addr = status.data['leader']
                if (new_leader_addr
                    and new_leader_addr[0] != -1 &
                    new_leader_addr[1] != leader['port']):
                    break
        self.assertNotEqual(new_leader_addr[0], -1,
                            msg="Leader election started but did not complete")
        self.assertNotEqual(new_leader_addr[1], leader['port'],
                            msg="Leader election never happend")
        logger.info("new leader found %s", new_leader_addr)
        balance = new_client.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        new_client.do_credit(10)
        balance = new_client.do_query()
        self.assertEqual(balance, "Your current account balance is: 20")
        logger.info("all operations working after election")

