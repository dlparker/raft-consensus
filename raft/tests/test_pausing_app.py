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
    
    def test_monitor_callbacks(self):
        # just make sure that all the monitors get called
        self.cluster.prep_mem_servers()
        monitors = []
        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            monitor = mserver.monitor
            monitors.append(monitor)

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
        leader_mon = None
        first_mon = None
        second_mon = None
        def do_pause():
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(get_timer_set()[0].pause_all())
            #do_pause()
        for monitor in monitors:
            if monitor.state is None:
                continue
            if str(monitor.state) == "leader":
                leader_mon = monitor
            elif first_mon is None:
                first_mon = monitor
            else:
                second_mon = monitor
        self.assertIsNotNone(leader_mon)
        self.assertIsNotNone(first_mon)
        self.assertIsNotNone(second_mon)
        run_data = run_data_from_status(self.cluster, self.logger, status)
        leader = run_data.leader
        first = run_data.first_follower
        second = run_data.second_follower
        addr = leader_mon.state_map.server.endpoint
        self.assertEqual(addr, leader['addr'])
        leader['monitor'] = leader_mon
        addr = first_mon.state_map.server.endpoint
        if addr == first['addr']:
            first['monitor'] = first_mon
            second['monitor'] = second_mon
        else:
            first['monitor'] = second_mon
            second['monitor'] = first_mon
            second_mon = first_mon
            first_mon = first['monitor']
        
    def test_pause_at_election_done_by_substate(self):
        self.cluster.prep_mem_servers()
        monitors = []

        servers = []
        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            servers.append(mserver)
            mserver.configure()
            monitor = mserver.monitor
            monitor.set_pause_on_substate(Substate.joined)
            monitor.set_pause_on_substate(Substate.new_leader)
            monitor.set_pause_on_substate(Substate.became_leader)
            monitors.append(monitor)
        self.cluster.start_all_servers()
        
        self.logger.info("waiting for pause on election results")
        start_time = time.time()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            pause_count = 0
            for server in servers:
                if server.paused:
                    pause_count += 1
            if pause_count == 3:
                break
        self.assertEqual(pause_count, 3,
                         msg=f"only {pause_count} servers pause on election")

        async def resume():
            for server in servers:
                await server.resume_all()

        self.loop.run_until_complete(resume())

    def test_pause_at_election_done_by_message(self):
        self.cluster.prep_mem_servers()
        interceptors = []

        servers = []

        # We want the leader to send both term start messages
        # so we need to count the outgoing until we have both,
        # then pause. So make a counter and pauser object
        # and supply it as the pause method
        class TwoCountPauser:

            def __init__(self, server):
                self.count = 0
                self.server = server

            async def pause(self, mode, code, message):
                self.count += 1
                if self.count < 2:
                    return True
                await self.server.pause_all(TriggerType.interceptor,
                                            dict(mode=mode, code=code))
                True
            
        for name,sdef in self.cluster.server_recs.items():
            mserver = sdef['memserver']
            servers.append(mserver)
            mserver.configure()
            inter = mserver.interceptor
            tcp = TwoCountPauser(mserver)
            inter.add_trigger(InterceptorMode.out_after,
                              TermStartMessage._code,
                              tcp.pause)
            inter.add_trigger(InterceptorMode.in_after,
                              TermStartMessage._code)
            interceptors.append(mserver.interceptor)
        self.cluster.start_all_servers()
        
        self.logger.info("waiting for pause on election results")
        start_time = time.time()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.25)
            pause_count = 0
            for server in servers:
                if server.paused:
                    pause_count += 1
            if pause_count == 3:
                break
        self.assertEqual(pause_count, 3,
                         msg=f"only {pause_count} servers pause on election")

        async def resume():
            for server in servers:
                await server.resume_all()

        self.loop.run_until_complete(resume())







        
            
