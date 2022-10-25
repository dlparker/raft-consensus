import unittest
import asyncio
import time
import logging
import traceback
import os
from dataclasses import dataclass
from typing import Union

from raft.dev_tools.ps_cluster import PausingServerCluster, PausePoint
from raft.dev_tools.udp_cluster import UDPServerCluster
from raft.dev_tools.ps_cluster import ServerSpec as PSSpec
from raft.dev_tools.udp_cluster import ServerSpec as UDPSpec

async def do_wait(seconds):
    start_time = time.time()
    while time.time() - start_time < seconds:
        await asyncio.sleep(0.01)

@dataclass
class RunData:
    leader: Union[PSSpec, UDPSpec]
    leader_addr: tuple
    first_follower: Union[PSSpec, UDPSpec]
    second_follower: Union[PSSpec, UDPSpec]

def run_data_from_status(cluster, logger, status):
    run_data = {}
    leader_addr = status.data['leader']
    leader = None
    first_follower = None
    second_follower = None
    for spec in cluster.get_servers().values():
        if spec.port == leader_addr[1]:
            spec.role = "leader"
            leader = spec
        else:
            if not first_follower:
                first_follower = spec
            else:
                second_follower = spec
            spec.role = "follower"
    logger.info("found leader %s", leader_addr)
    run_data = RunData(leader, leader_addr, first_follower, second_follower)
    return run_data
    
class BaseCase:

    class TestThreeServers(unittest.TestCase):
        
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
                
        def tearDown(self):
            pass
        
        def loop_setup(self):
            if self.get_process_flag():
                c = UDPServerCluster(server_count=3,
                                     logging_type=self.get_logging_type(),
                                     base_port=5000)
            else:
                c = PausingServerCluster(server_count=3,
                                         logging_type=self.get_logging_type(),
                                         base_port=5000)
                
            self.cluster = c
            if self.logger is None:
                self.logger = logging.getLogger(__name__)
            self.servers = self.cluster.prepare(timeout_basis=0.5)
            self.cluster.start_all_servers()
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)

        def loop_teardown(self):
            self.cluster.stop_all_servers()
            time.sleep(0.5)
            self.loop.close()

        def get_loop_limit(self):
            return 1
        
        def test_non_leader_stop(self):
            self.logger.info("starting test_non_leader_stop")
            for i in range(self.get_loop_limit()):
                if self.get_loop_limit() > 1:
                    print(f"\n\n\t\tstarting test_non_leader_stop loop {i}\n\n")
                self.loop_setup()
                self.inner_test_non_leader_stop()
                self.loop_teardown()

        def test_non_leader_restart(self):
            self.logger.info("starting test_non_leader_restart")
            for i in range(self.get_loop_limit()):
                if self.get_loop_limit() > 1:
                    print(f"\n\n\t\tstarting test_non_leader_restart loop {i}\n\n")
                self.loop_setup()
                self.inner_test_non_leader_stop(restart=True)
                self.loop_teardown()
                
        def test_leader_stop(self):
            self.logger.info("starting test_leader_stop")
            for i in range(self.get_loop_limit()):
                if self.get_loop_limit() > 1:
                    print(f"\n\n\t\tstarting test_leader_stop loop {i}\n\n")
                self.loop_setup()
                self.inner_test_leader_stop()
                self.loop_teardown()
            
        def test_leader_restart(self):
            self.logger.info("starting test_leader_restart")
            for i in range(self.get_loop_limit()):
                if self.get_loop_limit() > 1:
                    print(f"\n\n\t\tstarting test_leader_restart loop {i}\n\n")
                self.loop_setup()
                self.inner_test_leader_stop(restart=True)
                self.loop_teardown()

            
        def wait_for_election_done(self, client, old_leader=None, timeout=3):
            self.logger.info("waiting for election results")
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(0.01)
                status = client.get_status()
                if status and status.data['leader']:
                    new_leader_addr = status.data['leader']
                    if old_leader:
                        # might be either tuple or list
                        if old_leader[1] != new_leader_addr[1]:
                            break
                    else:
                        break
                status = None
            self.assertIsNotNone(status)
            self.assertIsNotNone(status.data['leader'])
            return run_data_from_status(self.cluster, self.logger, status)

        def do_op_seq_1(self, client1, client2):
            self.logger.info("doing credit at %s", client1)
            client1.do_credit(10)
            self.logger.info("doing query of %s", client1)
            result = client1.do_query()
            self.assertEqual(result['balance'], 10)
            self.logger.info("initial call via %s worked", client1)
            # get a client for the first follower
            self.logger.info("doing query of %s", client2)
            result = client2.do_query()
            self.assertEqual(result['balance'], 10)
            self.logger.info("initial call via %s worked", client2)
            
        def do_op_seq_2(self, client):
            self.logger.info("doing query of %s", client)
            result = client.do_query()
            self.assertEqual(result['balance'], 10)
            self.logger.info("doing credit at %s", client)
            client.do_credit(10)
            self.logger.info("doing query of %s", client)
            result2 = client.do_query()
            self.assertEqual(result2['balance'], 20)
            self.logger.info("all operations working pass 2")
            
        def do_op_seq_3(self, client):
            self.logger.info("doing query of %s", client)
            result = client.do_query()
            self.assertEqual(result['balance'], 20)
            self.logger.info("doing credit at %s", client)
            client.do_credit(10)
            self.logger.info("doing query of %s", client)
            result = client.do_query()
            self.assertEqual(result['balance'], 30)
            self.logger.info("all operations working pass 3")
            
        def do_restart(self, server_name):
            self.logger.info("restarting server %s", server_name)
            self.cluster.prepare_one(server_name)
            self.cluster.start_one_server(server_name)
            self.logger.info("restarted server, waiting for startup")
            self.cluster.wait_for_state("any", server_name)
            spec = self.servers[server_name]
            status_exc = None
            start_time = time.time()
            while time.time() - start_time < 4:
                self.loop.run_until_complete(do_wait(0.01))
                try:
                    restart_client = spec.get_client()
                    status = restart_client.get_status()
                    if status:
                        status_exc = None
                        break
                except Exception as e:
                    status_exc = e
            self.assertIsNone(status_exc, msg="Restart Failed!")
            return restart_client
            
        def inner_test_leader_stop(self, restart=False):
            spec_0 = self.servers["server_0"]
            self.cluster.wait_for_state()
            client0 =  spec_0.get_client()
            run_data = self.wait_for_election_done(client0)
            leader = run_data.leader
            first = run_data.first_follower
            new_client = first.get_client()
            leader_client = leader.get_client()
            orig_leader_addr = leader.addr
            self.do_op_seq_1(leader_client, new_client)
            self.logger.info("stopping leader server %s %s",
                             leader.name, orig_leader_addr)
            self.cluster.stop_server(leader.name)
            self.logger.info("!!!LEADER SERVER %s STOPPED!!!", leader.name)
            time.sleep(0.1)

            # wait for election to happen
            re_run_data = self.wait_for_election_done(new_client,
                                                      orig_leader_addr, 7)
            new_leader_addr = re_run_data.leader_addr
            self.assertNotEqual(new_leader_addr, orig_leader_addr,
                                msg="Leader election never happend")
            self.logger.info("new leader found %s", new_leader_addr)

            self.do_op_seq_2(new_client)
            
            if not restart:
                return
            self.do_restart(leader.name)
            self.do_op_seq_3(new_client)
            
        def inner_test_non_leader_stop(self, restart=False):
            spec_0 = self.servers["server_0"]
            self.cluster.wait_for_state()
            client1 =  spec_0.get_client()
            run_data = self.wait_for_election_done(client1)
            leader = run_data.leader
            first = run_data.first_follower
            second = run_data.second_follower
            client2 = first.get_client()
            self.do_op_seq_1(client1, client2)

            self.logger.info("stopping non_leader server %s",
                             second.name)

            self.cluster.stop_server(second.name)
            self.logger.info("        !!!NON LEADER SERVER %s STOPPED!!!    ",
                             second.name)

            self.do_op_seq_2(client1)
            
            if not restart:
                return
            self.do_restart(second.name)
            self.do_op_seq_3(client1)

    class TestClientOps(unittest.TestCase):
        
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

        def tearDown(self):
            pass

        def loop_setup(self):
            if self.get_process_flag():
                c = UDPServerCluster(server_count=3,
                                     logging_type=self.get_logging_type(),
                                     base_port=5000)
            else:
                c = PausingServerCluster(server_count=3,
                                         logging_type=self.get_logging_type(),
                                         base_port=5000)
                
            self.cluster = c
            if self.logger is None:
                self.logger = logging.getLogger(__name__)
            self.servers = self.cluster.prepare()
            self.cluster.start_all_servers()
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)

        def loop_teardown(self):
            self.cluster.stop_all_servers()
            time.sleep(0.5)
            self.loop.close()

        def get_loop_limit(self):
            return 1
            
        def wait_for_election_done(self, client, old_leader=None, timeout=3):
            self.logger.info("waiting for election results")
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(0.01)
                status = client.get_status()
                if status and status.data['leader']:
                    new_leader_addr = status.data['leader']
                    if old_leader:
                        if old_leader != new_leader_addr:
                            break
                    else:
                        break
                status = None
            
            self.assertIsNotNone(status)
            self.assertIsNotNone(status.data['leader'])
            return run_data_from_status(self.cluster, self.logger, status)

        def inner_test_client_ops(self):
            time.sleep(0.1)
            spec_0 = self.servers["server_0"]
            client1 =  spec_0.get_client()
            logged_command_count = 0
            run_data = self.wait_for_election_done(client1)
            
            self.logger.info("doing credit at %s", client1)
            client1.do_credit(10)
            logged_command_count += 1
            self.logger.info("doing query of %s", client1)
            result = client1.do_query()
            logged_command_count += 1
            self.assertEqual(result['balance'], 10)
            self.logger.info("doing debit at %s", client1)
            client1.do_debit(5)
            logged_command_count += 1
            self.logger.info("doing query of %s", client1)
            result = client1.do_query()
            logged_command_count += 1
            self.assertEqual(result['balance'], 5)
            result = client1.do_log_stats()
            self.assertEqual(result['last_index'], logged_command_count - 1)
            self.logger.info("client ops via %s worked", client1)
            
        def test_client_ops(self):
            for i in range(self.get_loop_limit()):
                if self.get_loop_limit() > 1:
                    print(f"\n\n\t\tstarting test_leader_stop loop {i}\n\n")
                self.loop_setup()
                self.inner_test_client_ops()
                self.loop_teardown()
