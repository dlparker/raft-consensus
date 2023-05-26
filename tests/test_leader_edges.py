import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raftframe.messages.request_vote import RequestVoteMessage
from raftframe.messages.request_vote import RequestVoteResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage
from raftframe.messages.heartbeat import HeartbeatMessage
from raftframe.messages.append_entries import AppendResponseMessage
from raftframe.log.log_api import LogRec
from raftframe.states.base_state import Substate
from raftframe.states.leader import Leader
from raftframe.states.leader import FollowerCursor
from raftframe.serializers.msgpack import MsgpackSerializer as Serializer
from dev_tools.memory_comms import MemoryComms
from dev_tools.pcluster import PausingCluster

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2

class BranchTricksLeader(Leader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.terminate_on_start = False
        self.broken = False

    async def on_start(self):
        self.terminated = self.terminate_on_start
        await super().on_start()


class TestTerminated(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingCluster(server_count=3,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=timeout_basis)
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
        # start two servers and wait for election to complete
        pserver0, pserver1, pserver2 = self.cluster.servers
        pserver0.pause_on_substate(Substate.joined)
        pserver0.pause_on_substate(Substate.sent_heartbeat)
        pserver1.pause_on_substate(Substate.joined)
        pserver1.pause_on_substate(Substate.sent_heartbeat)
        pserver0.start()
        pserver1.start()
        self.logger.info("waiting for election")
        leader = None
        follower = None
        start_time = time.time()

        # wait until one of the servers gets a synced substate
        # meaning it has gotten term start from the leader
        leader_paused = False
        follower_paused = False
        while time.time() - start_time < 2:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.05)
            for pserver in [pserver0, pserver1]:
                if pserver.state_map.get_state() is not None:
                    state = pserver.state_map.get_state()
                    if str(state) == "leader":
                        leader = pserver
                        if pserver.paused:
                            leader_paused = True
                    elif str(state) == "follower":
                        follower = pserver
                        if pserver.paused:
                            follower_paused = True
            if leader_paused and follower_paused:
                break
        self.assertTrue(leader_paused)
        self.assertTrue(follower_paused)
        return leader, follower
        
    def test_a_terminated_blocks(self):
        leader_server, follower_server = self.preamble()
        leader = leader_server.state_map.get_state()
        # just make sure address returned makes sense
        self.logger.info("Leader paused, going to call stop on it")
        async def try_stop():
            await leader.stop()
        self.loop.run_until_complete(try_stop())
        self.logger.info("Terminating Leader, calling start should raise")
        self.assertTrue(leader.terminated)
        with self.assertRaises(Exception) as context:
            leader.start()
        self.assertTrue("terminated" in str(context.exception))
        leader_server.pause_on_substate(Substate.sent_heartbeat)
        leader_server.clear_pause_on_substate(Substate.joined)
        follower_server.pause_on_substate(Substate.sent_heartbeat)
        follower_server.pause_on_substate(Substate.joined)
        leader_server.resume()
        follower_server.resume()


class TestOddPaths(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.cluster = PausingCluster(server_count=3,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=timeout_basis)
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
        # get a fully setup server, just ignore the existing state
        pserver = self.preamble()
        
        leader = BranchTricksLeader(pserver.thread.server)
        leader.terminate = True
        # make sure that terminated leader does not
        # allow start
        leader.terminated = True
        with self.assertRaises(Exception) as context:
            leader.start()
        self.assertTrue("terminated" in str(context.exception))
        leader.terminated = False
        # make sure that a terminated flag that sneaks in between
        # the sync call to start and the async start of on_start
        # results in no start
        leader.terminate_on_start = True
        async def do_start():
            leader.start()
        self.loop.run_until_complete(do_start())
        time.sleep(0.001)
        self.assertIsNone(leader.start_task)
        self.assertEqual(leader.substate, Substate.starting)
        self.assertFalse(leader.heartbeat_timer.keep_running)

        
    def test_bad_appends(self):
        # get a fully setup server, and change the state to leader
        # from follower
        pserver = self.preamble()
        follower = pserver.state_map.get_state()
        follower.terminated = True
        follower.leaderless_timer.terminated = True
        f_server = pserver.thread.server
        self.assertEqual(len(f_server.get_handled_errors()), 0)
        leader = Leader(f_server)
        pserver.state_map.state = leader
        log = f_server.get_log()
        term = log.get_term()
        # should hit the case where log is empty, leader doesn't
        # know what the hell we are talking about
        async def do_strange_arm1():
            leader.cursors[(0,1)] = FollowerCursor((0,1), None)
            resp = AppendResponseMessage((0, 1),
                                         (0,0),
                                         term,
                                         {
                                             "success": True,
                                             "last_entry_index": 0,
                                          }, 0, 0, 0)
            await leader.on_append_response(resp)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(do_strange_arm1())
        self.assertEqual(len(f_server.get_handled_errors()), 1)
        err = f_server.get_handled_errors(clear=True)[0]
        self.assertTrue("empty" in err['details'])

        # put a couple of records in the log and send a response
        # that seems to be from a bigger log
        new_rec = LogRec(term=log.get_term(),
                         user_data={'foo': 'bar'})
        log.append([new_rec,new_rec])
        async def do_strange_arm2():
            resp = AppendResponseMessage((0,1),
                                         (0,0),
                                         term,
                                         {
                                             "success": True,
                                             "last_entry_index": 3,
                                             "leaderCommit": 0,
                                         }, 0, 0, 0)
            await leader.on_append_response(resp)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(do_strange_arm2())
        self.assertEqual(len(f_server.get_handled_errors()), 1)
        err = f_server.get_handled_errors(clear=True)[0]
        self.assertTrue("claims record" in err['details'])
        
        # Send a log pull and capture the result to make sure it is
        # right
        # add more records, that makes 4
        log.append([new_rec,new_rec])
        class FakeServer:
            
            def __init__(self):
                self.in_queue = asyncio.Queue()
                
            async def on_message(self, message):
                await self.in_queue.put(message)

            def get_serializer(self):
                return Serializer
            
        server_1 = FakeServer()
        comms = MemoryComms()
        # set back to follower so shutdown will work
        pserver.state_map.state = follower

    def test_odd_msgs(self):
        # get a fully setup server, and change the state to leader
        # from follower
        pserver = self.preamble()
        follower = pserver.state_map.get_state()
        follower.terminated = True
        follower.leaderless_timer.terminated = True
        f_server = pserver.thread.server
        self.assertEqual(len(f_server.get_handled_errors()), 0)
        leader = Leader(f_server)
        pserver.state_map.state = leader
        log = f_server.get_log()
        # some message methods care about term
        term = 1
        log.set_term(term)

        async def allow_msgs():
            await pserver.resume_new_messages()
        self.loop.run_until_complete(allow_msgs())
        
        self.assertEqual(len(f_server.get_handled_errors()), 0)
        async def do_append_entries():
            msg = AppendEntriesMessage((0,1),
                                         (0,0),
                                         term,
                                         {}, 0, 0, 0)
            await leader.on_append_entries(msg)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(do_append_entries())
        self.assertEqual(len(f_server.get_handled_errors()), 0)

        async def do_got_vote():
            msg = RequestVoteResponseMessage((0,1),
                                             (0,0),
                                             term,
                                             {})
            await leader.on_vote_received(msg)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(do_got_vote())
        self.assertEqual(len(f_server.get_handled_errors()), 0)


        async def do_beat():
            msg = HeartbeatMessage((0,1),
                                   (0,0),
                                   term,
                                   {}, 0, 0, 0)
            await leader.on_heartbeat(msg)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(do_beat())
        self.assertEqual(len(f_server.get_handled_errors()), 0)
        
        class FakeServer:
            
            def __init__(self):
                self.in_queue = asyncio.Queue()
                
            async def on_message(self, message):
                await self.in_queue.put(message)
        
            def get_serializer(self):
                return Serializer
            
        server_1 = FakeServer()
        comms = MemoryComms()
        async def do_vote():
            # use non for start index, to trigger that
            # tiny branch
            await comms.start(server_1, ('localhost',5001))
            message = RequestVoteMessage(('localhost',5001),
                                         ('localhost',5000),
                                         term,
                                         {}, 0, 0, 0)
            await leader.on_vote_request(message)
            await asyncio.sleep(0.01)
            start_time = time.time()
            while time.time() - start_time < 0.1:
                await asyncio.sleep(0.001)
                if not server_1.in_queue.empty():
                    break
            self.assertFalse(server_1.in_queue.empty())
            reply = await server_1.in_queue.get()
            return reply
        reply = self.loop.run_until_complete(do_vote())
        self.assertTrue("already_leader" in reply.data)
        # set back to follower so shutdown will work
        pserver.state_map.state = follower
