import unittest
import asyncio
import time
import logging
import traceback
import os

from raft.tests.timer import get_timer_set
from raft.log.log_api import LogRec
from raft.log.memory_log import MemoryLog
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
        fo = Follower()
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


        from raft.states.timer import Timer
        def my_interval():
            return 10
        async def runner():
            t1 = Timer(my_interval, None)
            self.assertEqual(t1.get_interval(), 10)
            t2 = Timer(20, None)
            self.assertEqual(t2.get_interval(), 20)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(runner())
            

class TestMemoryLog(unittest.TestCase):

    def test_mem_log(self):
        pass        
        
