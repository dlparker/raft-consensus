import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.tests.bt_server import MemoryBankTellerServer
from raft.tests.bt_client import MemoryBankTellerClient
from raft.states.state_map import StandardStateMap, StateChangeMonitor
from raft.tests.common_test_code import RunData, run_data_from_status
from raft.tests.setup_utils import Cluster

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
    raft_log = logging.getLogger("raft")
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
        bt_server = MemoryBankTellerServer(0, Path('.'), "foo", (1,2),
                                        None, False)
        smap = bt_server.state_map = StandardStateMap()
        with self.assertRaises(Exception) as context:
            smap.get_state()
        with self.assertRaises(Exception) as context:
            await smap.switch_to_follower()
        with self.assertRaises(Exception) as context:
            await smap.switch_to_candidate()
        with self.assertRaises(Exception) as context:
            await smap.switch_to_leader()
            
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
        candidate = await smap.switch_to_candidate()
        await candidate.candidate_timer.terminate()
        leader = await smap.switch_to_leader()
        await leader.heartbeat_timer.stop()
        bt_server.stop()

        
