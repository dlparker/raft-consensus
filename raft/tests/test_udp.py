import os
import unittest
import asyncio
import time
import logging
import traceback

from raft.tests.timer import get_timer_set
from raft.tests.setup_utils import Cluster
from raft.tests.bt_client import UDPBankTellerClient
from raft.states.log_api import LogRec
from raft.states.memory_log import MemoryLog
from raft.states.follower import Follower
from raft.messages.regy import get_message_registry

#LOGGING_TYPE = "devel_one_proc" when using Mem comms and thread based servers
#LOGGING_TYPE = "devel_mp" when using UDP comms and MP process based servers
#LOGGING_TYPE = "silent" for no log at all
LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")

if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_mp" 
        
class TestThreeServers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = Cluster(server_count=3, use_processes=True,
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
            
        client1 =  UDPBankTellerClient("localhost", 5000)
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
        client2 =  UDPBankTellerClient("localhost", first_follower['port'])

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
        async def do_wait(seconds):
            start_time = time.time()
            while time.time() - start_time < seconds:
                await asyncio.sleep(0.01)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        logger = logging.getLogger(__name__)
        logger.info("starting test_leader_stop")
        client1 =  UDPBankTellerClient("localhost", 5000)
        status = None
        logger.info("waiting for election results")
        start_time = time.time()
        while time.time() - start_time < 3:
            loop.run_until_complete(do_wait(0.25))
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
        client2 =  UDPBankTellerClient("localhost", 
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
            loop.run_until_complete(do_wait(0.25))
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

        #loop.run_until_complete(do_wait(1))
        balance = new_client.do_query()
        self.assertEqual(balance, "Your current account balance is: 10")
        res = new_client.do_credit(10)
        balance = new_client.do_query()
        self.assertEqual(balance, "Your current account balance is: 20")
        logger.info("all operations working after election")

