import unittest
import asyncio
import time
import logging
import traceback
import os

from raft.tests.timer import get_timer_set, ControlledTimer
from raft.log.log_api import LogRec
from raft.log.memory_log import MemoryLog
from raft.states.timer import Timer
from raft.states.follower import Follower
from raft.messages.regy import get_message_registry

#LOGGING_TYPE = "devel_one_proc" when using Mem comms and thread based servers
#LOGGING_TYPE = "devel_mp" when using UDP comms and MP process based servers
#LOGGING_TYPE = "silent" for no log at all
LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")

if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc" 

class TestUtils(unittest.TestCase):

    def test_messages(self):

        from raft.messages.base_message import BaseMessage
        from raft.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
        b_msg = BaseMessage('1', '2', 0, "{'x':1}", '3')
        str_version = str(b_msg)
        dict_version = b_msg.props_as_dict()
        for key,value in dict_version.items():
            if key in ['data', 'original_sender']:
                continue
            self.assertTrue(str(value) in str_version)
        # these two have the data in the string rep
        rv_msg = RequestVoteMessage('1', '2', 0, "{'x':1}")
        rvr_msg = RequestVoteResponseMessage('1', '2', 0, "{'x':1}")
        for t_msg in [rv_msg, rvr_msg]:
            str_version = str(t_msg)
            dict_version = t_msg.props_as_dict()
            for key,value in dict_version.items():
                if key in ['original_sender']:
                    continue
                self.assertTrue(str(value) in str_version)
        
        regy = get_message_registry()
        hb_c = regy.get_message_class("heartbeat")
        self.assertIsNotNone(hb_c)
        msg = hb_c('1', '2', 0, "{'x':1}")
        # dummy objects to get follower to init enough
        # to use it as a handler source 
        class ftimer:
            def start(self):
                return
        class dserver:
            def get_timer(self, name, interval, function):
                return ftimer()
        fo = Follower(dserver())
        mh = regy.get_handler(msg, fo)
        self.assertIsNotNone(mh)
        expected_codes = ['heartbeat', 'heartbeat_response', 
                 'status_query', 'status_query_response', 
                 ]
        
        codes = regy.get_message_codes()
        for code in expected_codes:
            self.assertTrue(code in codes)
        
        sqr_c = regy.get_message_class("status_query_response")
        self.assertIsNotNone(sqr_c)
        msg = sqr_c('1', '2', 0, "{'x':1}")
        sqr_h = regy.get_handler(msg, fo)
        self.assertIsNone(sqr_h)
        all_classes = regy.get_message_classes()
        from raft.messages.heartbeat import HeartbeatMessage
        self.assertTrue(HeartbeatMessage in all_classes)
        from raft.messages.heartbeat import HeartbeatResponseMessage
        # this should be legal, a re-register
        regy.register_message_class(HeartbeatMessage, "on_heartbeat_response")
        # this should not, conflicting values
        class Dummy(BaseMessage):
            _code = "heartbeat"
            def __init__(self, sender, receiver, term, data):
                BaseMessage.__init__(self, sender, receiver, term, data)
        with self.assertRaises(Exception) as context:
            regy.register_message_class(Dummy, "on_heartbeat")

        
    def test_utils(self):
        # this just tests some utility functions that may not be called
        # otherwise, such as __str__ functions that are only used in
        # debug logging
        # get to BaseMessage through RequestVoteMessage
        from raft.messages.request_vote import RequestVoteMessage
        msg = RequestVoteMessage("sender", "target", 10, "data")
        self.assertTrue("sender" in str(msg))
        self.assertTrue("target" in str(msg))
        self.assertTrue("10" in str(msg))

        from raft.messages.serializer import Serializer
        # mess up the type of the vote message
        msg._code = "foo"
        bad_data = Serializer.serialize(msg)
        with self.assertRaises(Exception) as context:
            new_msg = Serializer.deserialize(bad_data)



class TestMemoryLog(unittest.TestCase):

    def test_mem_log(self):
        mlog = MemoryLog()
        rec = mlog.read()
        self.assertIsNone(rec)
        self.assertEqual(mlog.get_term(), None)
        mlog.incr_term()
        self.assertEqual(mlog.get_term(), 0)
        limit1 = 100
        for i in range(limit1):
            rec = LogRec(term=1, user_data=dict(index=i))
            mlog.append([rec,])
        with self.assertRaises(Exception) as context:
            mlog.commit(111)
        with self.assertRaises(Exception) as context:
            mlog.commit(-2)

        mlog.commit(0)
        self.assertEqual(mlog.get_commit_index(), 0)
        rec1 = mlog.read(1)
        self.assertFalse(rec1.committed)
        for i in range(1, limit1):
            mlog.commit(i)
        self.assertEqual(mlog.get_commit_index(), 99)
        for i in range(limit1):
            rec = mlog.read(i)
            self.assertTrue(rec.committed)
        self.assertIsNone(mlog.read(100))

        mlog.trim_after(50)
        rec = mlog.read()
        self.assertEqual(rec.index, 50)
        self.assertEqual(mlog.get_commit_index(), 50)

        mlog.set_term(10)
        self.assertEqual(mlog.get_term(), 10)
        mlog.incr_term()
        self.assertEqual(mlog.get_term(), 11)
        
class TestTimer(unittest.TestCase):

    def setUp(self):
        self.counter = 0

    async def target(self):
        self.counter += 1

    async def inner_test_timer_1(self):
        self.counter = 0
        t1 = Timer('foo', 0.05, self.target)
        t1.start()
        await asyncio.sleep(0.06)
        self.assertTrue(self.counter > 0)
        await t1.terminate()
        
        self.counter = 0
        t2 = Timer('bar', 0.05, self.target)
        start_time = time.time()
        t2.start()
        while time.time() - start_time < 0.06:
            await asyncio.sleep(0.005)
            await t2.reset()
        # should not have fired before we interupted with reset
        self.assertEqual(self.counter, 0)
        await asyncio.sleep(0.06)
        self.assertTrue(self.counter > 0)

        # make sure restart restarts if timer already stopped
        await t2.stop()
        # make sure stopping twice does not get error
        await t2.stop()
        self.counter = 0
        await t2.reset()
        start_time = time.time()
        while time.time() - start_time < 0.06:
            await asyncio.sleep(0.01)
        self.assertTrue(self.counter > 0)

        
        await t2.terminate()
        with self.assertRaises(Exception) as context:
            t1.start()
        self.assertTrue("start" in str(context.exception))
        self.assertTrue("terminated" in str(context.exception))
        with self.assertRaises(Exception) as context:
            await t1.stop()
        self.assertTrue("stop" in str(context.exception))
        self.assertTrue("terminated" in str(context.exception))
        with self.assertRaises(Exception) as context:
            await t1.reset()
        self.assertTrue("reset" in str(context.exception))
        self.assertTrue("terminated" in str(context.exception))
        with self.assertRaises(Exception) as context:
            await t1.terminate()
        self.assertTrue("terminate " in str(context.exception))
        self.assertTrue("terminated" in str(context.exception))
            
        # test that terminated state prevents callback
        # dummy objects to get follower to init enough
        # to use it as for timer check of terminated state 
        class ftimer:
            def start(self):
                return
        class dserver:
            def get_timer(self, name, interval, function):
                return ftimer()
        fo = Follower(dserver())
        
        t3 = Timer('on_term', 0.05, self.target, fo)
        # make sure it works once
        t3.start()
        await asyncio.sleep(0.06)
        await t3.stop() 
        self.assertTrue(self.counter > 0)

        # now change follower state to terminated
        fo.terminated = True
        self.counter = 0
        await t3.reset()
        await asyncio.sleep(0.06)
        await t3.terminate() 
        self.assertEqual(self.counter, 0)

        # Now make sure that an exception in the execution of
        # the one_pass method will not break the timer
        class Exploder(Timer):
            fuse = -1
            async def one_pass(self):
                self.fuse += 1
                if self.fuse == 1:
                    raise Exception("I die!")
                await super().one_pass()
                
        
        self.counter = 0
        t4 = Exploder('boom', 0.05, self.target)
        # first pass should work
        t4.start()
        await asyncio.sleep(0.06)
        self.assertEqual(self.counter, 1)
        # on second pass one_pass should blow up and callback won't happen
        # explode happens right away, and recalling one_pass happens right
        # away too, so don't wait long
        await asyncio.sleep(0.01)
        self.assertEqual(self.counter, 1)
        # third pass should work
        await asyncio.sleep(0.06)
        self.assertTrue(self.counter > 1)
        await t4.terminate()

        # Now make sure that an exception happens if the
        # loop won't stop for some strange reason
        # the one_pass method will not break the timer
        self.runner = True
        class Runaway(Timer):

            async def run(self):
                    while self.keep_running:
                        self.start_time = time.time()
                        try:
                            await self.one_pass()
                        except:
                            self.logger.error(traceback.format_exc())

                        while self.runner:
                            await asyncio.sleep(0.01)
                        self.task = None

        t5 = Runaway('boom', 0.05, self.target)
        # first pass should work
        t5.start()
        self.counter = 0
        await asyncio.sleep(0.06)
        self.assertEqual(self.counter, 1)
        with self.assertRaises(Exception) as context:
            await t5.stop()
        # cleanup
        t5.runner = False
        await t5.stop()
        await t5.terminate()

        
    def test_timer_1(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self.inner_test_timer_1())
        
    async def inner_test_controlled_timer_1(self):
        self.counter = 0
        name1 = "test1"
        t1 = ControlledTimer(name1, 0.05, self.target)
        t1.start()
        await asyncio.sleep(0.06)
        self.assertTrue(self.counter > 0)
        # now "pause" it through the controller api
        gset = get_timer_set()
        await gset.pause_by_name(name1)
        self.counter = 0
        await asyncio.sleep(0.1)
        self.assertEqual(self.counter, 0)
        gset.resume_by_name(name1)
        await asyncio.sleep(0.06)
        self.assertTrue(self.counter > 0)

        gset.pause_all()
        name2 = "test2"
        t2 = ControlledTimer(name2, 0.05, self.target)
        t2.start()
        # make sure new one is running
        await asyncio.sleep(0.06)
        self.assertTrue(self.counter > 0)
        await gset.pause_all()
        self.counter = 0
        await asyncio.sleep(0.1)
        self.assertEqual(self.counter, 0)
        gset.resume_all()
        await asyncio.sleep(0.6)
        self.assertTrue(self.counter > 1)
        
        await t1.terminate()
        await t2.terminate()

    def test_controlled_timer_1(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self.inner_test_controlled_timer_1())
        

        
