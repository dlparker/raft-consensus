import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from dev_tools.bt_server import MemoryBankTellerServer
from dev_tools.bt_client import MemoryBankTellerClient
from raftframe.states.state_map import StandardStateMap
from raftframe.app_api.app import StateChangeMonitor
from raftframe.states.base_state import Substate

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    logging.root.handlers = []
    lfstring = '%(process)s %(asctime)s [%(levelname)s] %(name)s: %(message)s'
    logging.basicConfig(format=lfstring,
                        level=logging.DEBUG)

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    raft_log = logging.getLogger("raftframe")
    raft_log.setLevel(logging.DEBUG)


class TestMap(unittest.TestCase):

    def test_methods(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.inner_test_methods())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()
        
    async def inner_test_methods(self):
        bt_server = MemoryBankTellerServer(0, Path('.'), "foo",
                                           (('l',1),('l',2)), None)
        smap = bt_server.state_map = StandardStateMap()
        # make sure all errors raise when calls made
        # before activate call
        with self.assertRaises(Exception) as context:
            smap.get_state()
        self.assertTrue("must call activate" in str(context.exception))
        with self.assertRaises(Exception) as context:
            await smap.set_substate("","")
        self.assertTrue("must call activate" in str(context.exception))
        with self.assertRaises(Exception) as context:
            smap.get_substate()
        self.assertTrue("must call activate" in str(context.exception))
        with self.assertRaises(Exception) as context:
            await smap.switch_to_follower()
        self.assertTrue("must call activate" in str(context.exception))
        with self.assertRaises(Exception) as context:
            await smap.switch_to_candidate()
        self.assertTrue("must call activate" in str(context.exception))
        with self.assertRaises(Exception) as context:
            await smap.switch_to_leader()
        self.assertTrue("must call activate" in str(context.exception))

        # make sure add_state_change_monitor works even
        # if no activate call yet
        class FakeMonitor(StateChangeMonitor):
            state = None
            substate = None
            async def new_state(self, state_map, old_state, new_state):
                self.state = new_state
                return new_state
            async def new_substate(self, state_map, state, substate):
                self.substate = substate

        class BadMonitor(StateChangeMonitor):
            state = None
            substate = None
            async def new_state(self, state_map, old_state, new_state):
                raise Exception("testing error capture in state map")
            async def new_substate(self, state_map, state, substate):
                raise Exception("testing error capture in state map")

        fake_mon = FakeMonitor()
        bad_mon = BadMonitor()
        smap.add_state_change_monitor(fake_mon)
        self.assertTrue(len(smap.monitors) > 0)
        smap.add_state_change_monitor(bad_mon)
        self.assertTrue(len(smap.monitors) > 1)
        bt_server.start()
        start_time = time.time()
        while time.time() - start_time < 0.5 and smap.state is None:
            await asyncio.sleep(0.0001)
        self.assertIsNotNone(smap.state)
        follower = smap.state
        # now terminate the timer so that we can test things
        # without them changing
        await follower.leaderless_timer.terminate()
        server = bt_server.thread.server
        self.assertEqual(smap.get_server(), server)
        self.assertEqual(smap.get_state(), follower)
        self.assertEqual(server.get_state(), follower)
        self.assertEqual(fake_mon.state, follower)
        with self.assertRaises(Exception) as context:
            await smap.set_substate(None, Substate.starting)
        self.assertTrue("non-current state" in str(context.exception))
        await smap.set_substate(follower, Substate.starting)
        self.assertEqual(smap.get_substate(), Substate.starting)
        self.assertEqual(fake_mon.substate, Substate.starting)
        candidate = await smap.switch_to_candidate()
        await candidate.candidate_timer.terminate()
        leader = await smap.switch_to_leader()
        await leader.heartbeat_timer.stop()
        bt_server.stop()

        # Just a little sequence to excersize the normal
        # path of activate being called before adding state change
        # monitors
        bt_server_2 = MemoryBankTellerServer(1, Path('.'), "foo", (1,2),
                                             None)
        bt_server_2.start()
        smap2 = bt_server_2.state_map
        start_time = time.time()
        while time.time() - start_time < 0.5 and smap2.state is None:
            await asyncio.sleep(0.0001)
        self.assertIsNotNone(smap2.state)
        smap2.add_state_change_monitor(fake_mon)
        self.assertTrue(len(smap2.monitors) > 0)
        bt_server_2.stop()
        
