import unittest
import asyncio
import time
import logging
import traceback
import os
from dataclasses import dataclass

from raft.comms.memory_comms import MemoryComms
from raft.states.timer import Timer
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
        
        end_1 = MemoryComms()
        server_1 = FakeServer()
        end_2 = MemoryComms()
        server_2 = FakeServer()

        async def do_seq1():
            await end_1.start(server_1, (0, 0))
            await end_2.start(server_2, (0, 1))
            msg1 = StatusQueryMessage(end_1.endpoint, end_2.endpoint,
                                      term=0, data=dict(foo="bar"))
            await end_1.post_message(msg1)
            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0.001)
                if not end_1.are_out_queues_empty():
                    continue
                if not server_2.in_queue.empty():
                    break
            
            self.assertFalse(server_2.in_queue.empty())
            msg1_sent = await server_2.in_queue.get()
            reply_1 = StatusQueryResponseMessage(end_2.endpoint,
                                                 end_1.endpoint,
                                                 term=0,
                                                 data=msg1_sent.data)
            await end_2.post_message(reply_1)
            
            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0.001)
                if not end_2.are_out_queues_empty():
                    continue
                if not server_1.in_queue.empty():
                    break
            self.assertFalse(server_1.in_queue.empty())
            reply_sent = await server_1.in_queue.get()
            self.assertEqual(reply_sent.data, msg1.data)
            await end_1.stop()
            await end_2.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_seq1())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()
        
    def test_delayed_start(self):
        self.logger.info("starting test_delayed_start")
        
        end_1 = MemoryComms()
        server_1 = FakeServer()
        end_2 = MemoryComms()
        server_2 = FakeServer()


        async def do_seq2():
            # post a message before starting end_2
            await end_1.start(server_1, (0, 0))
            msg1 = StatusQueryMessage(end_1.endpoint, (0, 1),
                                      term=0, data=dict(foo="bar"))
            # The post message method will wait for the target endpoint
            # to show up, so we don't want to wait for it. Give it a
            # bit to start checking and then launch the target
            task = asyncio.create_task(end_1.post_message(msg1))
            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0)
                if end_1.out_message_pending:
                    break
            self.assertTrue(end_1.out_message_pending)
            await end_2.start(server_2, (0, 1))

            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0.001)
                if not end_1.are_out_queues_empty():
                    continue
                if not server_2.in_queue.empty():
                    break
            
            self.assertFalse(server_2.in_queue.empty())
            msg1_sent = await server_2.in_queue.get()
            reply_1 = StatusQueryResponseMessage(end_2.endpoint,
                                                 end_1.endpoint,
                                                 term=0,
                                                 data=msg1_sent.data)
            await end_2.post_message(reply_1)
            
            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0.001)
                if not end_2.are_out_queues_empty():
                    continue
                if not server_1.in_queue.empty():
                    break
            self.assertFalse(server_1.in_queue.empty())
            reply_sent = await server_1.in_queue.get()
            self.assertEqual(reply_sent.data, msg1.data)
            await end_1.stop()
            await end_2.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_seq2())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()

class TestDebugControls(unittest.TestCase):
        
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

    def test_pause_on_send(self):
        self.logger.info("starting test_pause_on_send")
        
        end_1 = MemoryComms()
        server_1 = FakeServer()
        end_2 = MemoryComms()
        server_2 = FakeServer()

        async def do_seq1():
            await end_1.start(server_1, (0, 0))
            await end_2.start(server_2, (0, 1))
            msg1 = StatusQueryMessage(end_1.endpoint, end_2.endpoint,
                                      term=0, data=dict(foo="bar"))
            self.assertFalse(end_1.out_message_pending)
            # if we set pause, outgoing should be pending but
            # not sent
            end_1.pause()
            asyncio.create_task(end_1.post_message(msg1))
            await asyncio.sleep(.01)
            self.assertTrue(server_2.in_queue.empty())
            # resume and it should go
            end_1.resume()
            await asyncio.sleep(.01)
            self.assertFalse(server_2.in_queue.empty())
            
            await end_1.stop()
            await end_2.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_seq1())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()

    def test_pause_on_reply(self):
        self.logger.info("starting test_pause_on_send")
        
        end_1 = MemoryComms()
        server_1 = FakeServer()
        end_2 = MemoryComms()
        server_2 = FakeServer()

        async def do_seq1():
            await end_1.start(server_1, (0, 0))
            await end_2.start(server_2, (0, 1))
            msg1 = StatusQueryMessage(end_1.endpoint, end_2.endpoint,
                                      term=0, data=dict(foo="bar"))
            self.assertFalse(end_1.out_message_pending)
            # if we set pause on end_2, outgoing from end_1 should go
            # to queue, but incomming on end_2 should deuque
            end_2.pause()
            await end_1.post_message(msg1)
            await asyncio.sleep(.01)
            self.assertFalse(end_1.are_out_queues_empty())
            # resume and it should deliver
            end_2.resume()
            await asyncio.sleep(.01)
            self.assertTrue(end_1.are_out_queues_empty())
            self.assertFalse(server_2.in_queue.empty())
            await end_1.stop()
            await end_2.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_seq1())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()
        
        
    def test_out_msg_holder(self):
        self.logger.info("starting test_out_msg_holder")
        
        end_1 = MemoryComms()
        server_1 = FakeServer()
        end_2 = MemoryComms()
        server_2 = FakeServer()

        self.held_message = None
        async def holder(msg):
            # could do some sort of delay or debug action here
            self.held_message = msg
            
        async def do_seq1():
            end_1.set_out_message_holder(holder)
            await end_1.start(server_1, (0, 0))
            await end_2.start(server_2, (0, 1))
            msg1 = StatusQueryMessage(end_1.endpoint, end_2.endpoint,
                                      term=0, data=dict(foo="bar"))
            await end_1.post_message(msg1)
            self.assertIsNotNone(self.held_message)
            await end_1.stop()
            await end_2.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_seq1())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()

    def test_in_msg_holder(self):
        self.logger.info("starting test_in_msg_holder")
        
        end_1 = MemoryComms()
        server_1 = FakeServer()
        end_2 = MemoryComms()
        server_2 = FakeServer()

        self.held_message = None
        async def holder(msg):
            # could do some sort of delay or debug action here
            self.held_message = msg
            
        async def do_seq1():
            end_2.set_in_message_holder(holder)
            await end_1.start(server_1, (0, 0))
            await end_2.start(server_2, (0, 1))
            msg1 = StatusQueryMessage(end_1.endpoint, end_2.endpoint,
                                      term=0, data=dict(foo="bar"))
            await end_1.post_message(msg1)
            await asyncio.sleep(0.001)
            self.assertIsNotNone(self.held_message)
            await end_1.stop()
            await end_2.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_seq1())
        except KeyboardInterrupt:
            pass
        logging.info('Closing the loop')
        loop.close()

