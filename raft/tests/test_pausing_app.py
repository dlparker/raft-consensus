import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.termstart import TermStartMessage
from raft.dev_tools.pausing_app import InterceptorMode, TriggerType
from raft.states.base_state import Substate
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
        self.cluster = PausingServerCluster(server_count=3,
                               logging_type=LOGGING_TYPE,
                               base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.logger = logging.getLogger(__name__)
                
    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.1)
        self.loop.close()
    
    def test_pause_at_election_done_by_substate(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.add_pause_point(PausePoint.election_done)

        for spec in servers.values():
            # follower after first heartbeat that requires no
            # additional sync up actions
            spec.monitor.set_pause_on_substate(Substate.synced)
            # leader after term start message
            spec.monitor.set_pause_on_substate(Substate.sent_heartbeat)
        self.cluster.start_all_servers()
        self.logger.info("waiting for pause on election results")
        start_time = time.time()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
            if pause_count == 3:
                break
        self.assertEqual(pause_count, 3,
                         msg=f"only {pause_count} servers paused on election")

        for spec in servers.values():
            spec.monitor.clear_pause_on_substate(Substate.synced)
            spec.monitor.clear_pause_on_substate(Substate.sent_heartbeat)
        self.cluster.resume_all_paused_servers()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
            if pause_count == 0:
                break
        self.assertEqual(pause_count, 0)
        
    def test_pause_at_election_done_by_message(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)

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
                return True
            
        for spec in servers.values():
            inter = spec.pbt_server.interceptor
            tcp = TwoCountPauser(spec.pbt_server)
            inter.add_trigger(InterceptorMode.out_after,
                              TermStartMessage._code,
                              tcp.pause)
            inter.add_trigger(InterceptorMode.in_after,
                              TermStartMessage._code)
        self.cluster.start_all_servers()
        
        self.logger.info("waiting for pause on election results")
        start_time = time.time()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
            if pause_count == 3:
                break
        self.assertEqual(pause_count, 3,
                         msg=f"only {pause_count} servers pause on election")

        for spec in servers.values():
            inter = spec.pbt_server.interceptor
            tcp = TwoCountPauser(spec.pbt_server)
            inter.clear_trigger(InterceptorMode.out_after,
                              TermStartMessage._code)
            inter.clear_trigger(InterceptorMode.in_after,
                                TermStartMessage._code)
        self.cluster.resume_all_paused_servers()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
            if pause_count == 0:
                break
        self.assertEqual(pause_count, 0)
  






        
            
