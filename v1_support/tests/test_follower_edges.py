import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raftframe.messages.append_entries import AppendEntriesMessage
from raftframe.messages.heartbeat import HeartbeatMessage
from raftframe.messages.command import ClientCommandMessage
from raftframe.log.log_api import LogRec
from raftframe.states.base_state import Substate
from raftframe.states.follower import Follower
from raftframe.states.state_map import StateMap
from dev_tools.memory_comms import MemoryComms
from dev_tools.pcluster import PausingCluster

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
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.logger = logging.getLogger("tests." + __name__)
                
    def tearDown(self):
        self.cluster.stop_all()
        time.sleep(0.1)
        self.loop.close()
        
    def preamble(self):
        self.cluster = PausingCluster(server_count=3,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=timeout_basis)
        server = self.cluster.servers[0]
        # Wait for startup so that we know that
        # server is initialized. The current state
        # will be folower, but we are not going to let it run
        server.pause_on_substate(Substate.leader_lost)
        server.start()
        self.logger.info("waiting for switch to leader_lost")
        start_time = time.time()
        while time.time() - start_time < 3 and not server.paused:
            time.sleep(0.05)
        self.assertTrue(server.paused)
        return server
    
    def test_quick_stop(self):
        # get a fully setup server
        pserver = self.preamble()
        follower = pserver.state_map.get_state()
        t_follower = Follower(pserver.thread.server, follower.timeout)
        t_follower.terminated = True
        with self.assertRaises(Exception) as context:
            t_follower.start()
        self.assertTrue("terminated" in str(context.exception))
        self.assertIsNone(t_follower.leaderless_timer)


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
        self.cluster = PausingCluster(server_count=3,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=timeout_basis)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
                
    def tearDown(self):
        self.loop.close()
    
    def test_direct_termination(self):
        pserver = self.cluster.servers[0]
        pserver.pause_on_substate(Substate.leader_lost)
        pserver.start()
        self.logger.info("waiting for leader lost pause")
        start_time = time.time()
        while time.time() - start_time < 1.5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if pserver.paused:
                break
        self.assertTrue(pserver.paused)
        # Now terminate it and resume, which should not result in an
        # election because of the terminated flag
        pre_state = pserver.state_map.get_state()
        pre_state.terminated = True
        pserver.resume()
        time.sleep(0.25)
        # if election started, state will have changed to candidate
        follower = pserver.state_map.get_state()
        self.assertEqual(pre_state, follower)
        
        # Now try to force the election to start and ensure it does
        # not due to the terminated flag
        async def start_election():
            await follower.start_election()
        self.loop.run_until_complete(start_election())
        self.assertEqual(pre_state, pserver.state_map.get_state())

        # Now make sure trying to start it raises
        with self.assertRaises(Exception) as context:
            follower.start()
        self.assertTrue("terminated" in str(context.exception))

        async def do_stop():
            await follower.stop()
            
        # Unset the termination flag and then call stop.
        # Should terminate the timer
        follower.terminated = False
        self.loop.run_until_complete(do_stop())
        self.assertIsNone(follower.leaderless_timer)
        pserver.stop()
