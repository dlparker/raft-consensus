import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.heartbeat import HeartbeatMessage
from raft.messages.heartbeat import HeartbeatResponseMessage
from raft.messages.append_entries import AppendEntriesMessage
from raft.messages.append_entries import AppendResponseMessage
from raft.messages.request_vote import RequestVoteMessage
from raft.messages.request_vote import RequestVoteResponseMessage
from raft.messages.status import StatusQueryResponseMessage
from raft.states.base_state import Substate, StateCode
from raft.dev_tools.bt_client import MemoryBankTellerClient
from raft.dev_tools.ps_cluster import PausingServerCluster
from raft.dev_tools.pausing_app import PausingMonitor, PFollower, PLeader
from raft.dev_tools.timer_wrapper import get_all_timer_sets

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2

class ExplodingLeader(PLeader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        super().start()
        self.termintated = True
        logger = logging.getLogger(__name__)
        logger.info("\n\n\t exploding \n\n")
        raise Exception('boom')

    async def on_start(self):
        self.heartbeat_timer.allow_failed_terminate = True
        await self.heartbeat_timer.terminate()
        self.task = None
        
        
class ExplodingFollower(PFollower):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        super().start()
        self.leaderless_timer.allow_failed_terminate = True
        raise Exception('boom')

class ExplodingMonitor(PausingMonitor):

    def __init__(self, orig_monitor):
        super().__init__(orig_monitor.pbt_server,
                         orig_monitor.name,
                         orig_monitor.logger)
        self.state_map = orig_monitor.state_map
        self.state = orig_monitor.state
        self.substate = orig_monitor.substate
        self.pbt_server.state_map.remove_state_change_monitor(orig_monitor)
        self.pbt_server.state_map.add_state_change_monitor(self)
        self.explode = False
        self.leader = None
        self.follower = None
        
    async def new_state(self, state_map, old_state, new_state):
        if new_state.code == StateCode.leader and self.explode:
            new_state = ExplodingLeader(new_state.server,
                                         new_state.heartbeat_timeout)
            self.leader = new_state
            return new_state
        if new_state.code == StateCode.follower and self.explode:
            new_state = ExplodingFollower(new_state.server,
                                           new_state.timeout)
            self.follower = new_state
            return new_state
        new_state = await super().new_state(state_map, old_state, new_state)
        return new_state
    
class TestOddMsgArrivals(unittest.TestCase):

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
        # start just the one server and wait for it
        # to pause in candidate state
        spec = servers["server_0"]
        monitor = spec.monitor
        monitor.set_pause_on_substate(Substate.voting)
        self.cluster.start_one_server("server_0")
        self.logger.info("waiting for switch to candidate")
        start_time = time.time()
        while time.time() - start_time < 3:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.05)
            if monitor.state is not None:
                if str(monitor.state) == "candidate":
                    if monitor.substate == Substate.voting:
                        if monitor.pbt_server.paused:
                            break
        self.assertEqual(monitor.substate, Substate.voting)
        self.assertTrue(monitor.pbt_server.paused)
        return spec
        
    def test_a_terminated_blocks(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        self.logger.info("Candidate paused, going to call stop on it")
        async def try_stop():
            await candidate.stop()
        self.loop.run_until_complete(try_stop())
        self.assertTrue(candidate.terminated)
        self.logger.info("Calling stop again, should raise")
        with self.assertRaises(Exception) as context:
            candidate.start()
        self.assertTrue("terminated" in str(context.exception))
        # If the initial check for terminated fails because
        # someone removes it or changes the logic, then the
        # call to self.candidate_timer.start() will raise
        # an exception, so just making the call to
        # start_election is a test. No raise means no problem
        self.logger.info("Trying start election while terminated")
        self.assertTrue(candidate.candidate_timer.terminated)
        async def try_election():
            await candidate.start_election()
        self.loop.run_until_complete(try_election())
        # If the initial check for terminated fails because
        # someone removes it or changes the logic, then the
        # call to resign will result in a state_map change
        # to follower.
        self.logger.info("Trying resign while terminated")
        # make sure it does not blow up on seeing the timer
        # is terminated, fake it to look like it is still running
        candidate.candidate_timer.terminated = False
        async def try_resign():
            await candidate.resign()
        self.loop.run_until_complete(try_resign())
        # make sure it did not switch
        self.assertEqual(spec.pbt_server.state_map.state, candidate)
        self.logger.info("State did not change, test passed, cleaning up")
        monitor.clear_pause_on_substate(Substate.voting)
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        
        
    def test_b_msg_ignores(self):
        spec = self.preamble()
        monitor = spec.monitor
        server = spec.server_obj # this is the servers/server.py Server
        # leave the timer off but allow comms 
        client =  MemoryBankTellerClient("localhost", 5000)
        status = client.get_status()
        self.assertIsNotNone(status)
        tsm = RequestVoteMessage(("localhost", 5001),
                               ("localhost", 5000),
                               0,
                               {})
        self.logger.info("Sending request vote message" \
                         " expecting no error ignore")
        client.direct_message(tsm)
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        tsm = AppendResponseMessage(("localhost", 5001),
                                    ("localhost", 5000),
                                    0,
                                    {})
        self.logger.info("Sending AppendEntries response message" \
                         " expecting no error ignore")
        client.direct_message(tsm)
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        tsm = HeartbeatResponseMessage(("localhost", 5001),
                                           ("localhost", 5000),
                                           0,
                                           {})
        self.logger.info("Sending heartbeat response message" \
                         " expecting no error ignore")
        client.direct_message(tsm)
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        
    def test_b_msg_rejects(self):
        spec = self.preamble()
        monitor = spec.monitor
        server = spec.server_obj # this is the servers/server.py Server
        # leave the timer off but allow comms 
        self.logger.info("Candidate paused, sending status query")
        client =  MemoryBankTellerClient("localhost", 5000)
        status = client.get_status()
        res = client.do_credit(10)
        self.assertTrue('not available' in res)
        self.logger.info("Status query returned 'not available' as expected")
        self.logger.info("Setting candidate to terminated")
        monitor.state.terminated = True
        self.assertTrue(monitor.state.is_terminated())
        # any old message would do
        tsm = HeartbeatMessage(("localhost", 5001),
                               ("localhost", 5000),
                               0,
                               {})
        self.logger.info("Sending heartbeat message expecting reject")
        client.direct_message(tsm)
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        start_time = time.time()
        while time.time() - start_time < 3:
            time.sleep(0.05)
            if len(server.unhandled_errors) > 0:
                break
        self.assertEqual(len(server.get_unhandled_errors()), 1)
        self.logger.info("Reject resulted in server saving error\n%s",
                         server.get_unhandled_errors()[0])
        self.logger.info("clearing terminated flag")
        monitor.state.terminated = False
        # Now send a message that has no handler.
        # The StatusQuery message sequence is designed for client
        # to server, not server to server, so server does not expect
        # to get response, so no handler registered
        self.logger.info("Sending message that has no registered handler")
        sqrm = StatusQueryResponseMessage(("localhost", 5001),
                                          ("localhost", 5000),
                                          0,
                                          {})
        client.direct_message(sqrm)
        start_time = time.time()
        while time.time() - start_time < 3:
            time.sleep(0.05)
            if len(server.unhandled_errors) > 1:
                break
        self.assertEqual(len(server.get_unhandled_errors()), 2)
        self.logger.info("Reject resulted in server saving error\n%s",
                         server.get_unhandled_errors()[1])
        
        self.logger.info("starting other two servers and waiting for election")
        monitor.clear_substate_pauses()
        async def do_resume():
            await monitor.pbt_server.resume_all()
        self.loop.run_until_complete(do_resume())
        self.cluster.start_one_server("server_1")
        self.cluster.start_one_server("server_2")
        time.sleep(0.1)
        leader_add = None
        start_time = time.time()
        while time.time() - start_time < 3:
            time.sleep(0.05)
            status = client.get_status()
            if status and status.data['leader']:
                leader_addr = status.data['leader']
                break
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        self.logger.info("Elected %s, test passed", status.data['leader'])
            
    def release_to_resign(self, spec, method):
        monitor = spec.monitor
        candidate = monitor.state
        monitor.clear_pause_on_substate(Substate.voting)
        monitor.set_pause_on_substate(Substate.leader_lost)
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        async def wait_for_pause():
            start_time = time.time()
            while time.time() - start_time < 0.5:
                await asyncio.sleep(0.001)
                if spec.pbt_server.paused:
                    return
            raise Exception("timeout waiting for pause")
        self.loop.run_until_complete(wait_for_pause())
        self.loop.run_until_complete(method())
        monitor.clear_pause_on_substate(Substate.leader_lost)

    def test_b_heartbeat_reject(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        async def try_heartbeat():
            hbm = HeartbeatMessage(("localhost", 5001),
                                   ("localhost", 5000),
                                   0,
                                   {
                                       "leaderId": "server_1",
                                       "leaderPort": ("localhost", 5001),
                                       "prevLogIndex": None,
                                       "prevLogTerm": None,
                                       "entries": [],
                                       "leaderCommit": None,
                                   })
            await candidate.on_heartbeat(hbm)
            await asyncio.sleep(0)
        self.release_to_resign(spec, try_heartbeat)
        self.assertFalse(monitor.state == candidate)
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        
    def test_b_got_append(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        async def try_append():
            aem = AppendEntriesMessage(("localhost", 5001),
                                       ("localhost", 5000),
                                       0,
                                       {
                                           "leaderId": "server_1",
                                           "leaderPort": ("localhost", 5001),
                                           "prevLogIndex": None,
                                           "prevLogTerm": None,
                                           "entries": [],
                                           "leaderCommit": None,
                                       })
            await candidate.on_append_entries(aem)
            await asyncio.sleep(0)
        self.release_to_resign(spec, try_append)
        self.assertFalse(monitor.state == candidate)
        self.loop.run_until_complete(spec.pbt_server.resume_all())

    def test_c_on_timer(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        async def try_on_timer():
            await candidate.on_timer()
            await asyncio.sleep(0)
        self.release_to_resign(spec, try_on_timer)
        self.assertFalse(monitor.state == candidate)
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        
    def test_b_on_append(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        async def try_append():
            aem = AppendEntriesMessage(("localhost", 5001),
                                       ("localhost", 5000),
                                       0,
                                       {
                                           "leaderId": "server_1",
                                           "leaderPort": ("localhost", 5001),
                                           "prevLogIndex": None,
                                           "prevLogTerm": None,
                                           "entries": [],
                                           "leaderCommit": None,
                                       })
            await candidate.on_append_entries(aem)
            await asyncio.sleep(0)
        self.release_to_resign(spec, try_append)
        self.assertFalse(monitor.state == candidate)
        self.loop.run_until_complete(spec.pbt_server.resume_all())

    def test_vote_response_1(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        async def try_vote_response():
            vrm = RequestVoteResponseMessage(("localhost", 5001),
                                             ("localhost", 5000),
                                             0,
                                             {
                                                 "response": False,
                                                 "already_leader": True
                                             })
            await candidate.on_vote_received(vrm)
            await asyncio.sleep(0)
        self.release_to_resign(spec, try_vote_response)
        self.assertFalse(monitor.state == candidate)
        self.loop.run_until_complete(spec.pbt_server.resume_all())

    def test_vote_response_2(self):
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        async def try_vote_response():
            vrm = RequestVoteResponseMessage(("localhost", 5001),
                                             ("localhost", 5000),
                                             0,
                                             {
                                                 "response": False,
                                             })
            await candidate.on_vote_received(vrm)
            vrm._sender = ("localhost", 5002)
            await candidate.on_vote_received(vrm)
            await asyncio.sleep(0)
        self.release_to_resign(spec, try_vote_response)
        self.assertFalse(monitor.state == candidate)
        self.loop.run_until_complete(spec.pbt_server.resume_all())

    def test_vote_response_3(self):
        # two votes from the same sender should not cause a resign
        spec = self.preamble()
        monitor = spec.monitor
        candidate = monitor.state
        async def try_revote_response():
            vrm = RequestVoteResponseMessage(("localhost", 5001),
                                             ("localhost", 5000),
                                             0,
                                             {
                                                 "response": False,
                                             })
            await candidate.on_vote_received(vrm)
            await candidate.on_vote_received(vrm)
            await asyncio.sleep(0)
        self.loop.run_until_complete(try_revote_response())
        self.assertEqual(monitor.state, candidate)
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        
    def test_c_resign_fail(self):
        # Make the resign blow up
        spec = self.preamble()
        server = spec.server_obj
        monitor = ExplodingMonitor(spec.monitor)
        spec.pbt_server.monitor = monitor
        spec.monitor = monitor
        monitor.explode = True
        candidate = monitor.state
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        async def try_resign():
            await candidate.resign()
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(try_resign())
        # state change should have failed
        self.assertEqual(monitor.state, candidate)
        self.assertEqual(len(server.get_unhandled_errors(clear=True)), 1)
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        monitor.explode = False
        candidate.terminated = False
        candidate.candidate_timer.terminated = False
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        self.loop.run_until_complete(try_resign())
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        self.assertNotEqual(monitor.state, candidate)
        monitor.state.leaderless_timer.allow_failed_terminate = True

    def test_c_promote_fail(self):
        # Make the promition to leader blow up
        spec = self.preamble()
        server = spec.server_obj
        monitor = ExplodingMonitor(spec.monitor)
        spec.pbt_server.monitor = monitor
        spec.monitor = monitor
        monitor.explode = True
        candidate = monitor.state
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        monitor.set_pause_on_substate(Substate.voting)
        async def try_vote_response():
            vrm = RequestVoteResponseMessage(("localhost", 5001),
                                             ("localhost", 5000),
                                             0,
                                             {
                                                 "response": True,
                                             })
            await candidate.on_vote_received(vrm)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(try_vote_response())
        # state change should have failed
        self.assertEqual(monitor.state, candidate)
        self.assertEqual(len(server.get_unhandled_errors(clear=True)), 1)
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        monitor.explode = False
        candidate.candidate_timer.terminated = False
        candidate.all_votes = {}
        monitor.clear_pause_on_substate(Substate.voting)
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        self.loop.run_until_complete(try_vote_response())
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        self.assertNotEqual(monitor.state, candidate)
        self.loop.run_until_complete(asyncio.sleep(0.05))
        # let it do stop without errors
        monitor.state.heartbeat_timer.allow_failed_terminate = True





        
            
