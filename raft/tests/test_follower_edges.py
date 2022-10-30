import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.append_entries import AppendEntriesMessage
from raft.messages.termstart import TermStartMessage
from raft.messages.heartbeat import HeartbeatMessage
from raft.messages.command import ClientCommandMessage
from raft.messages.log_pull import LogPullMessage, LogPullResponseMessage
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


    def test_odd_msgs(self):
        # get a fully setup server, and change the state to leader
        # from follower
        spec = self.preamble()
        monitor = spec.monitor
        follower = monitor.state
        server = spec.server_obj
        self.assertEqual(len(server.get_handled_errors()), 0)
        sm = server.get_state_map()
        # some message methods care about term
        term = 0

        self.assertEqual(len(server.get_handled_errors()), 0)
        msg = AppendEntriesMessage((0,1),
                                   (0,0),
                                   term,
                                   {})

        msg._data = dict(leaderCommit=None, prevLogIndex=None,
                           prevLogTerm=None, leaderAddr=(0,0))
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_term)
        self.assertTrue(res.same_index)
        self.assertTrue(res.same_commit)
        self.assertTrue(res.local_commit_none)
        self.assertTrue(res.leader_commit_none)
        self.assertFalse(res.local_ahead)
        self.assertTrue(res.local_index_none)
        self.assertTrue(res.leader_index_none)
        self.assertTrue(res.leader_prev_term_none)

        # now update the local term, should show in sync

        log = server.get_log()
        log.set_term(term)
        msg._term = term
        res = follower.decode_state(msg)
        self.assertTrue(res.same_term)
        self.assertTrue(res.same_index)
        self.assertTrue(res.same_commit)
        self.assertTrue(res.same_prev_term)
        self.assertTrue(res.in_sync)
        back_to = res.rollback_to
        self.assertIsNone(back_to)

        # Now claim that the leader has one log entry but still
        # no commit, that means we don't want any entries
        msg._data = dict(leaderCommit=None, prevLogIndex=0,
                           prevLogTerm=0,leaderAddr=(0,0))
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertTrue(res.same_term)
        self.assertFalse(res.same_index)
        self.assertTrue(res.same_commit)
        self.assertTrue(res.local_commit_none)
        self.assertTrue(res.local_index_none)
        self.assertFalse(res.local_ahead)
        self.assertTrue(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNone(needed)
        back_to = res.rollback_to
        self.assertIsNone(back_to)

        # Now claim that the leader has one log entry and it 
        # is committed, that means we want one entry starting
        # at index 0
        msg._data = dict(leaderCommit=0, prevLogIndex=0,
                           prevLogTerm=0, leaderAddr=(0,0))
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_index)
        self.assertFalse(res.same_commit)
        self.assertTrue(res.local_commit_none)
        self.assertTrue(res.local_index_none)
        self.assertFalse(res.local_ahead)
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 0)
        self.assertEqual(needed['end'], 0)
        back_to = res.rollback_to
        self.assertIsNone(back_to)
        
        # Now claim that the leader has two log entries and 
        # only one is committed, that means we want one entry starting
        # at index 0
        msg._data = dict(leaderCommit=0, prevLogIndex=1,
                           prevLogTerm=0, leaderAddr=(0,0))
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_index)
        self.assertFalse(res.same_commit)
        self.assertTrue(res.local_commit_none)
        self.assertTrue(res.local_index_none)
        self.assertFalse(res.local_ahead)
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 0)
        self.assertEqual(needed['end'], 0)
        back_to = res.rollback_to
        self.assertIsNone(back_to)
        
        # Now claim that the leader has two log entries and 
        # both are committed, that means we want two entries starting
        # at index 0
        msg._data = dict(leaderCommit=1, prevLogIndex=1,
                           prevLogTerm=0, leaderAddr=(0,0))
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_index)
        self.assertFalse(res.same_commit)
        self.assertTrue(res.local_commit_none)
        self.assertTrue(res.local_index_none)
        self.assertFalse(res.local_ahead)
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 0)
        self.assertEqual(needed['end'], 1)
        back_to = res.rollback_to
        self.assertIsNone(back_to)

        # Now add a record to the local log, and resend the last
        # message. This should result in "needed" saying we need
        # one entry starting at 1

        new_rec = LogRec(term=log.get_term(),
                         user_data={'foo': 'bar'})
        log.append([new_rec,])
        log.commit(0)
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_commit)
        self.assertFalse(res.local_commit_none)
        self.assertFalse(res.local_index_none)
        self.assertFalse(res.local_ahead)
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)

        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 1)
        self.assertEqual(needed['end'], 1)
        back_to = res.rollback_to
        self.assertIsNone(back_to)

        # Now add another record to the local log, and resend the last
        # message. This should result in "needed" saying need nothing,
        # and same_commit reporting True
        # one entry starting at 1
        log.append([new_rec,])
        log.commit(1)
        res = follower.decode_state(msg)
        self.assertTrue(res.same_index)
        self.assertTrue(res.same_term)
        self.assertTrue(res.same_prev_term)
        self.assertTrue(res.same_commit)
        self.assertTrue(res.in_sync)
        self.assertFalse(res.local_commit_none)
        self.assertFalse(res.local_index_none)
        self.assertFalse(res.local_ahead)
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)

        needed = res.needed_records
        self.assertIsNone(needed)
        back_to = res.rollback_to
        self.assertIsNone(back_to)
        
        # Now add another record to the local log, and resend the last
        # message. This should result in an indication that the local
        # state is ahead of the leader state, which requires a rollback
        log.append([new_rec,])
        log.commit(2)
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_commit)
        self.assertTrue(res.local_ahead)
        needed = res.needed_records
        self.assertIsNone(needed)
        back_to = res.rollback_to
        self.assertEqual(back_to, 1)
        

        # With three records committed to the local log, send
        # a message indicating that the leader has an empty log.
        # This is possible (though unlikely) until I add leader resignation
        # logic for re-configuration on merging segmented networks.
        # TODO: Remove this when that is done
        msg._data = dict(leaderCommit=None, prevLogIndex=None,
                           prevLogTerm=None, leaderAddr=(0,0))
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_commit)
        self.assertTrue(res.local_ahead)
        needed = res.needed_records
        self.assertIsNone(needed)
        back_to = res.rollback_to
        self.assertEqual(back_to, None)
        
        # With three records committed to the local log, send
        # a message indicating that the leader has one uncommitted
        # record in the log.
        # This is possible (though unlikely) until I add leader resignation
        # logic for re-configuration on merging segmented networks.
        # TODO: Remove this when that is done
        msg._data = dict(leaderCommit=None, prevLogIndex=0,
                           prevLogTerm=0, leaderAddr=(0,0))
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_commit)
        self.assertTrue(res.local_ahead)
        needed = res.needed_records
        self.assertIsNone(needed)
        back_to = res.rollback_to
        self.assertEqual(back_to, None)
        

        monitor.clear_pause_on_substate(Substate.leader_lost)
        self.loop.run_until_complete(spec.pbt_server.resume_all())

class TestLogOps(unittest.TestCase):

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

    def pause_waiter(self, label, expected=2, timeout=2):
        start_time = time.time()
        while time.time() - start_time < timeout:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in self.servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
                    if spec.monitor.state._type == "leader":
                        leader = spec
                    if spec.monitor.state._type == "follower":
                       follower = spec
            if pause_count == expected:
                break
        self.assertEqual(pause_count, expected,
                         msg=f"only {pause_count} servers paused on {label}")
        return leader, follower

    def resume_waiter(self):
        start_time = time.time()
        while time.time() - start_time < 4:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            pause_count = 0
            for spec in self.servers.values():
                if spec.pbt_server.paused:
                    pause_count += 1
            if pause_count == 0:
                break
        self.assertEqual(pause_count, 0)
        
    def test_it(self):
        self.servers = self.cluster.prepare(timeout_basis=timeout_basis)
        spec0 = self.servers["server_0"]
        spec1 = self.servers["server_1"]
        monitor0 = spec0.monitor
        monitor1 = spec1.monitor
        for spec in self.servers.values():
            spec.monitor.set_pause_on_substate(Substate.synced)
            spec.monitor.set_pause_on_substate(Substate.sent_heartbeat)
        self.cluster.start_one_server(spec0.name)
        self.cluster.start_one_server(spec1.name)
        self.logger.info("waiting for pause on election results")

        leader, follower = self.pause_waiter("election results")

        for spec in self.servers.values():
            spec.monitor.clear_pause_on_substate(Substate.synced)
            spec.monitor.clear_pause_on_substate(Substate.sent_heartbeat)


        leader.monitor.set_pause_on_substate(Substate.sent_new_entries)
                                         
        follower.interceptor.add_trigger(InterceptorMode.in_after,
                                         AppendEntriesMessage._code)
                                         
        self.cluster.resume_all_paused_servers()
        self.resume_waiter()
        
        # can't do this:
        #client = leader.get_client()
        #client.do_credit(10)
        # because it will timeout
        class FakeServer:

            def __init__(self):
                self.in_queue = asyncio.Queue()
                
            async def on_message(self, message):
                await self.in_queue.put(message)
        
        server_1 = FakeServer()
        comms = MemoryComms()
        my_addr = ('localhost', 6000)
        async def do_query():
            await comms.start(server_1, my_addr)
            command = ClientCommandMessage(my_addr,
                                           leader.server_obj.endpoint,
                                           None,
                                           "query")
            await comms.post_message(command)
        self.loop.run_until_complete(do_query())
        
        self.pause_waiter("update via append")
        # at this point append entries has run and follower should
        # have a record in its log
        flog = follower.server_obj.get_log()
        frec = flog.read()
        self.assertIsNotNone(frec)
        
        leader.monitor.clear_pause_on_substate(Substate.sent_new_entries)
        leader.monitor.set_pause_on_substate(Substate.sent_commit)
        self.cluster.resume_all_paused_servers()
        # they'll pause again quit, so don't try to wait:
        # self.resume_waiter()
        
        self.pause_waiter("commit via append")
        # At this point append entries for commit has run and follower
        # should have commited the record
        frec = flog.read()
        self.assertIsNotNone(frec)
        self.assertTrue(frec.committed)

        
        leader.monitor.clear_pause_on_substate(Substate.sent_commit)

        leader.interceptor.add_trigger(InterceptorMode.out_after,
                                       HeartbeatMessage._code)

        follower.interceptor.add_trigger(InterceptorMode.in_after,
                                         HeartbeatMessage._code)

        self.cluster.resume_all_paused_servers()
        self.pause_waiter("next append")
        # At this point leader should have sent reply
        self.reply = None
        async def get_reply():
            start_time = time.time()
            while time.time() - start_time < 2:
                if not server_1.in_queue.empty():
                    self.reply = await server_1.in_queue.get()
                    break
        self.loop.run_until_complete(get_reply())
        self.assertIsNotNone(self.reply)

        # add a couple of records to the leader log, then let
        # them run and log pull should happen
        
        llog = leader.server_obj.get_log()
        lrec = flog.read()
        llog.append([lrec,lrec])
        lrec = flog.read()
        # commit only the first one
        llog.commit(1)
        leader.monitor.clear_substate_pauses()
        leader.interceptor.clear_triggers()
        follower.monitor.clear_substate_pauses()
        follower.interceptor.clear_triggers()

        async def checker(mode, code, message):
            if message.data['leaderCommit'] == 1:
                await leader.pbt_server.pause_all(TriggerType.interceptor,
                                                  dict(mode=mode,
                                                       code=code))
        follower.monitor.set_pause_on_substate(Substate.debug_point)
        leader.interceptor.add_trigger(InterceptorMode.out_after,
                                       HeartbeatMessage._code,
                                       checker)


        self.cluster.resume_all_paused_servers()
        self.pause_waiter("log_pull")
        leader.interceptor.clear_triggers()
        follower.interceptor.clear_triggers()
        self.cluster.resume_all_paused_servers()
        if False:
            leader.interceptor.add_trigger(InterceptorMode.out_after,
                                           LogPullResponseMessage._code)
            
            follower.interceptor.add_trigger(InterceptorMode.out_after,
                                             LogPullMessage._code)
