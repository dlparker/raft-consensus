import os
import unittest
import asyncio
import time
import logging
import traceback
from enum import Enum
from dataclasses import dataclass

from raft.messages.status import StatusQueryMessage, StatusQueryResponseMessage
from raft.dev_tools.memory_comms import MemoryComms, MessageInterceptor
from raft.dev_tools.memory_comms import reset_channels

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

    def __init__(self):
        self.in_queue = asyncio.Queue()
        
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
        reset_channels()
    
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
            self.assertFalse(end_1.are_out_queues_empty())
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
            task = asyncio.create_task(end_1.post_message(msg1))
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

class InterceptorMode(str, Enum):
    in_before = "IN_BEFORE"
    out_before = "OUT_BEFORE"
    in_after = "IN_AFTER"
    out_after = "OUT_AFTER"
    
class MyInterceptor(MessageInterceptor):

    def __init__(self, logger, pause_method):
        self.logger = logger
        self.pause_method = pause_method
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}
        
    async def before_in_msg(self, message) -> bool:
        method = self.in_befores.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("Before message %s in calling method",
                              message.code)
            go_on = await method(InterceptorMode.in_before,
                           message.code,
                           message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            if message.code in self.in_befores:
                del self.in_befores[message.code]
        return go_on

    async def after_in_msg(self, message) -> bool:
        method = self.in_afters.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("after message %s in calling method",
                              message.code)
            go_on = await method(InterceptorMode.in_after,
                           message.code,
                           message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            if message.code in self.in_afters:
                del self.in_afters[message.code]
        return go_on

    async def before_out_msg(self, message) -> bool:
        method = self.out_befores.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("before message %s out calling method",
                              message.code)
            go_on = await method(InterceptorMode.out_before,
                          message.code,
                          message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            if message.code in self.out_befores:
                del self.out_befores[message.code]
        return go_on

    async def after_out_msg(self, message) -> bool:
        method = self.out_afters.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("after message %s out calling method",
                              message.code)
            go_on = await method(InterceptorMode.out_after,
                           message.code,
                           message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            if message.code in self.out_afters:
                del self.out_afters[message.code]
        return go_on
    
    def clear_triggers(self):
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}

    def add_trigger(self, mode, message_code, method=None):
        if method is None:
            method = self.pause_method
        if mode == InterceptorMode.in_before:
            self.in_befores[message_code] = method
        elif mode == InterceptorMode.in_after:
            self.in_afters[message_code] = method
        elif mode == InterceptorMode.out_before:
            self.out_befores[message_code] = method
        elif mode == InterceptorMode.out_after:
            self.out_afters[message_code] = method
        else:
            raise Exception(f"invalid mode {mode}")

        
class TestDebugControls(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger("raft.tests")
        cls.logger.info("")
        
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        reset_channels()
    
    def tearDown(self):
        pass

    def test_interceptor(self):

        self.logger.info("starting test_pause_on_send")
        
        end_1 = MemoryComms()
        server_1 = FakeServer()
        end_2 = MemoryComms()
        server_2 = FakeServer()
        pauses = []
        class Pauser:

            def __init__(self):
                self.skip_on = None
                self.hold_pause = {InterceptorMode.out_before: True,
                                   InterceptorMode.out_after: True,
                                   InterceptorMode.in_before: True,
                                   InterceptorMode.in_after: True}
            
            async def pause_method(self, mode, code, message):
                pauses.append(dict(mode=mode, code=code, message=message))
                while self.hold_pause[mode]:
                    await asyncio.sleep(0.001)
                if self.skip_on == mode:
                    return False
                return True

        pauser = Pauser()
        inter1 = MyInterceptor(self.logger, pauser.pause_method)
        inter1.add_trigger(InterceptorMode.out_before,
                           StatusQueryMessage._code,
                           pauser.pause_method)
        inter1.add_trigger(InterceptorMode.out_after,
                           StatusQueryMessage._code,
                           pauser.pause_method)
        end_1.set_interceptor(inter1)
        self.assertEqual(end_1.get_interceptor(), inter1)
        inter2 = MyInterceptor(self.logger, pauser.pause_method)
        inter2.add_trigger(InterceptorMode.in_before,
                           StatusQueryMessage._code,
                           pauser.pause_method)
        inter2.add_trigger(InterceptorMode.in_after,
                           StatusQueryMessage._code,
                           pauser.pause_method)
        end_2.set_interceptor(inter2)
        
        async def do_seq1():
            await end_1.start(server_1, (0, 0))
            await end_2.start(server_2, (0, 1))
            msg1 = StatusQueryMessage(end_1.endpoint, end_2.endpoint,
                                      term=0, data=dict(foo="bar"))
            asyncio.create_task(end_1.post_message(msg1))
            await asyncio.sleep(.01)
            self.assertEqual(len(pauses), 1)
            self.assertEqual(pauses[0]['mode'], InterceptorMode.out_before)
            pauser.hold_pause[InterceptorMode.out_before] = False
            await asyncio.sleep(.01)
            self.assertEqual(len(pauses), 3)
            self.assertEqual(pauses[1]['mode'], InterceptorMode.out_after)
            self.assertEqual(pauses[2]['mode'], InterceptorMode.in_before)
            pauser.hold_pause[InterceptorMode.out_before] = False
            pauser.hold_pause[InterceptorMode.in_before] = False
            await asyncio.sleep(.01)
            self.assertEqual(len(pauses), 4)
            self.assertEqual(pauses[3]['mode'], InterceptorMode.in_after)
            pauser.hold_pause[InterceptorMode.in_after] = False
            await asyncio.sleep(0)

            # Now do a pass where the pause method returns false
            # on before on the out side,  which will cause the message
            # not to be delivered to the queue.
            pauser.skip_on = InterceptorMode.out_before
            asyncio.create_task(end_1.post_message(msg1))
            await asyncio.sleep(.01)
            self.assertEqual(len(pauses), 5)
            pauser.hold_pause[InterceptorMode.out_before] = False
            await asyncio.sleep(.01)
            # should not see any more pauses
            self.assertEqual(len(pauses), 5)

            # Now do a pass where the pause method returns false
            # on before on the in side,  which will cause the message
            # not to be delivered to the server

            pauser.skip_on = InterceptorMode.in_before
            asyncio.create_task(end_1.post_message(msg1))
            await asyncio.sleep(.01)
            # will be out_before, out_after and in_before
            self.assertEqual(len(pauses), 8)
            pauser.hold_pause[InterceptorMode.in_before] = False
            await asyncio.sleep(.01)
            # should not see any more pauses
            self.assertEqual(len(pauses), 8)
            
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
