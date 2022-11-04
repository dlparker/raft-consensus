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


    def test_state_diffs(self):
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
        self.assertTrue(res.local_term_none)
        self.assertTrue(res.same_index)
        self.assertTrue(res.same_commit)
        self.assertTrue(res.local_commit_none)
        self.assertTrue(res.leader_commit_none)
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
        self.assertFalse(res.need_rollback)
        self.assertIsNone(res.rollback_to)

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
        self.assertTrue(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNone(needed)
        self.assertFalse(res.need_rollback)

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
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 0)
        self.assertEqual(needed['end'], 0)
        self.assertFalse(res.need_rollback)
        
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
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 0)
        self.assertEqual(needed['end'], 0)
        self.assertFalse(res.need_rollback)
        
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
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)
        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 0)
        self.assertEqual(needed['end'], 1)
        self.assertFalse(res.need_rollback)

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

        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)

        needed = res.needed_records
        self.assertIsNotNone(needed)
        self.assertEqual(needed['start'], 1)
        self.assertEqual(needed['end'], 1)
        self.assertFalse(res.need_rollback)

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
        self.assertFalse(res.leader_commit_none)
        self.assertFalse(res.leader_index_none)
        self.assertFalse(res.leader_prev_term_none)

        needed = res.needed_records
        self.assertIsNone(needed)
        self.assertFalse(res.need_rollback)

        
        # Now add another record to the local log, and resend the last
        # message. This should result in an indication that the local
        # state is ahead of the leader state, which requires a rollback
        log.append([new_rec,])
        log.commit(2)
        res = follower.decode_state(msg)
        self.assertFalse(res.in_sync)
        self.assertFalse(res.same_commit)
        needed = res.needed_records
        self.assertIsNone(needed)
        self.assertTrue(res.need_rollback)
        self.assertEqual(res.rollback_to, 1)
        

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
        needed = res.needed_records
        self.assertIsNone(needed)
        self.assertTrue(res.need_rollback)
        self.assertEqual(res.rollback_to, -1)
        
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
        needed = res.needed_records
        self.assertIsNone(needed)
        self.assertTrue(res.need_rollback)
        self.assertEqual(res.rollback_to, 0)

        monitor.clear_pause_on_substate(Substate.leader_lost)
        self.loop.run_until_complete(spec.pbt_server.resume_all())

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
        self.cluster = PausingServerCluster(server_count=3,
                                            logging_type=LOGGING_TYPE,
                                            base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
                
    def tearDown(self):
        self.loop.close()
    
    def test_direct_termination(self):
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        name = "server_0"
        spec = servers[name]
        spec.monitor.set_pause_on_substate(Substate.leader_lost)
        self.cluster.start_one_server(name)
        self.logger.info("waiting for leader lost pause")
        start_time = time.time()
        while time.time() - start_time < 1.5:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if spec.pbt_server.paused:
                break
        self.assertTrue(spec.pbt_server.paused)
        # Now terminate it and resume, which should not result in an
        # election because of the terminated flag
        pre_state = spec.monitor.state
        spec.monitor.state.terminated = True
        async def do_resume():
            await spec.pbt_server.resume_all()
        self.loop.run_until_complete(do_resume())
        time.sleep(0.25)
        # if election started, state will have changed to candidate
        self.assertEqual(pre_state, spec.monitor.state)

        # Now try to force the election to start and ensure it does
        # not due to the terminated flag
        async def start_election():
            await spec.monitor.state.start_election()
        self.loop.run_until_complete(start_election())
        self.assertEqual(pre_state, spec.monitor.state)

        # Now make sure trying to start it raises
        with self.assertRaises(Exception) as context:
            spec.monitor.state.start()
        self.assertTrue("terminated" in str(context.exception))

        async def do_stop():
            await spec.monitor.state.stop()
            
        # Unset the termination flag and then call stop.
        # Should terminate the timer
        spec.monitor.state.terminated = False
        self.loop.run_until_complete(do_stop())
        self.assertTrue(spec.monitor.state.leaderless_timer.terminated)
        self.cluster.stop_server(name)
