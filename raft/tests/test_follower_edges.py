import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.append_entries import AppendEntriesMessage
from raft.messages.heartbeat import HeartbeatMessage
from raft.messages.command import ClientCommandMessage
from raft.comms.memory_comms import MemoryComms
from raft.log.log_api import LogRec
from raft.states.base_state import Substate
from raft.dev_tools.ps_cluster import PausingServerCluster
from raft.dev_tools.pausing_app import PausingMonitor, PFollower
from raft.dev_tools.pausing_app import InterceptorMode, TriggerType

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2

class TestOddPaths(unittest.TestCase):

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
        self.logger = logging.getLogger("raft.tests." + __name__)
                
    def tearDown(self):
        self.cluster.stop_all_servers()
        time.sleep(0.1)
        self.loop.close()
        
    def preamble(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        spec = servers["server_0"]
        monitor = spec.monitor
        # Wait for startup so that we know that
        # server is initialized. The current state
        # will be folower, but we are not going to let it run
        monitor.set_pause_on_substate(Substate.leader_lost)
        self.cluster.start_one_server("server_0")
        self.logger.info("waiting for switch to leader_lost")
        start_time = time.time()
        while time.time() - start_time < 3 and not monitor.pbt_server.paused:
            time.sleep(0.05)
        self.assertTrue(monitor.pbt_server.paused)
        return spec
    
    def test_quick_stop(self):
        # get a fully setup server
        spec = self.preamble()
        monitor = spec.monitor
        monitor.state.stop()
        follower = PFollower(monitor.state.server,
                             monitor.state.timeout)
        monitor.state = follower 
        follower.terminated = True
        with self.assertRaises(Exception) as context:
            follower.start()
        self.assertTrue("terminated" in str(context.exception))
        self.assertIsNone(follower.leaderless_timer)
        follower.terminated = False


class TestStateEdges(unittest.TestCase):

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
        self.loop.close()
    
    def test_direct_termination(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        name = "server_0"
        spec = servers[name]
        spec.monitor.set_pause_on_substate(Substate.leader_lost)
        self.cluster.start_one_server(name)
        self.logger.info("waiting for leader lost pause")
        start_time = time.time()
        while time.time() - start_time < 1.5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if spec.pbt_server.paused:
                break
        self.assertTrue(spec.pbt_server.paused)
        # Now terminate it and resume, which should not result in an
        # election because of the terminated flag
        pre_state = spec.monitor.state
        spec.monitor.state.terminated = True
        async def do_resume():
            await spec.pbt_server.resume_all()
        self.loop.run_until_complete(do_resume())
        time.sleep(0.25)
        # if election started, state will have changed to candidate
        self.assertEqual(pre_state, spec.monitor.state)

        # Now try to force the election to start and ensure it does
        # not due to the terminated flag
        async def start_election():
            await spec.monitor.state.start_election()
        self.loop.run_until_complete(start_election())
        self.assertEqual(pre_state, spec.monitor.state)

        # Now make sure trying to start it raises
        with self.assertRaises(Exception) as context:
            spec.monitor.state.start()
        self.assertTrue("terminated" in str(context.exception))

        async def do_stop():
            await spec.monitor.state.stop()
            
        # Unset the termination flag and then call stop.
        # Should terminate the timer
        spec.monitor.state.terminated = False
        self.loop.run_until_complete(do_stop())
        self.assertTrue(spec.monitor.state.leaderless_timer.terminated)
        self.cluster.stop_server(name)
