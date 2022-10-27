import unittest
import asyncio
import time
import logging
import traceback
import os
from dataclasses import dataclass

from raft.comms.udp import UDPComms
from raft.messages.status import StatusQueryMessage, StatusQueryResponseMessage

#LOGGING_TYPE = "silent" for no log at all
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


class FakeServer:

    def __init__(self, timer_class=None):
        self.timer_class = timer_class
        self.in_queue = asyncio.Queue()
        
    def set_timer_class(self, timer_class):
        self.timer_class = timer_class

    async def on_message(self, message):
        await self.in_queue.put(message)
        
class TestBasic(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger("raft.tests")
        cls.logger.info("")
        
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    def test_simple_round_trip(self):
        self.logger.info("starting test_simple_round_trip")
        
        end_1 = UDPComms()
        server_1 = FakeServer()
        end_2 = UDPComms()
        server_2 = FakeServer()

        async def do_seq1():
            await end_1.start(server_1, ("localhost", 5000))
            await end_2.start(server_2, ("localhost", 5001))
            msg1 = StatusQueryMessage(end_1.endpoint, end_2.endpoint,
                                      term=0, data=dict(foo="bar"))
            await end_1.post_message(msg1)
            self.assertFalse(end_1.are_out_queues_empty())
            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0.001)
                if not server_2.in_queue.empty():
                    break
            
            self.assertFalse(server_2.in_queue.empty())
            msg1_sent = await server_2.in_queue.get()
            self.assertTrue(end_1.are_out_queues_empty())
            reply_1 = StatusQueryResponseMessage(end_2.endpoint,
                                                 end_1.endpoint,
                                                 term=0,
                                                 data=msg1_sent.data)
            await end_2.post_message(reply_1)
            
            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0.001)
                if not server_1.in_queue.empty():
                    break
            self.assertFalse(server_1.in_queue.empty())
            reply_sent = await server_1.in_queue.get()
            self.assertEqual(reply_sent.data, msg1.data)
            await end_1.stop()
            await asyncio.sleep(0.01)
            await end_2.stop()
            await asyncio.sleep(0.01)


        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_seq1())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()
        
