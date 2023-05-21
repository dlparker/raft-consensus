import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raftframe.messages.heartbeat import HeartbeatMessage
from raftframe.messages.heartbeat import HeartbeatResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage
from raftframe.messages.append_entries import AppendResponseMessage
from raftframe.messages.request_vote import RequestVoteMessage
from raftframe.messages.request_vote import RequestVoteResponseMessage
from raftframe.messages.status import StatusQueryResponseMessage
from raftframe.states.base_state import Substate, StateCode
from raftframe.states.state_map import StandardStateMap
from dev_tools.bt_client import MemoryBankTellerClient
from dev_tools.pcluster import PausingCluster

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.1

class ExplodingStateMap(StandardStateMap):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.explode_state_change = False

    async def switch_to_follower(self, old_state=None):
        if self.explode_state_change:
            raise Exception('boom')
        return await super().switch_to_follower(old_state)
        
    async def switch_to_leader(self, old_state=None):
        if self.explode_state_change:
            raise Exception('boom')
        return await super().switch_to_leader(old_state)
    

class TestOddMsgs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
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

    def preamble(self, slow=False, use_exploder=False):
        if slow:
            tb = 1.0
        else:
            tb = timeout_basis
        self.cluster = PausingCluster(server_count=3,
                                      logging_type=LOGGING_TYPE,
                                      base_port=5000,
                                      timeout_basis=tb)
        servers = self.cluster.servers
        # start just the one server and wait for it
        # to pause in candidate state
        server = servers[0]
        new_state_map = ExplodingStateMap(timeout_basis=server.state_map.timeout_basis)
        new_state_map.monitors = []
        for monitor in server.state_map.monitors:
            new_state_map.monitors.append(monitor)
        server.state_map = new_state_map
        async def pause_callback(pserver, context):
            state = pserver.state_map.get_state()
            self.logger.info(f'{pserver.name} {state} pausing')
            if str(state) == "leader":
                self.logger.info("I am paused in leader server")
            elif str(state) == "candidate":
                self.logger.info("I am paused in candidate server")
            if str(state) == "follower":
                self.logger.info("I am paused in follower server")

        async def resume_callback(pserver):
            self.logger.info(f'{pserver.name} resumed')
            
        server.pause_callback = pause_callback
        server.resume_callback = resume_callback
        # pause candidate at starto
        server.pause_on_substate(Substate.voting)
        server.start()
        self.logger.info("waiting for switch to candidate and pause")
        start_time = time.time()
        while time.time() - start_time < 10 * timeout_basis:
            # servers are in their own threads, so
            # blocking this one is fine
            time.sleep(0.01)
            if server.paused:
                break
        if not server.paused:
            server.stop()
        self.assertTrue(server.paused)
        self.assertEqual(server.state_map.get_substate(), Substate.voting)
        return server
        
    def test_a_terminated_blocks(self):
        pserver = self.preamble()
        candidate = pserver.state_map.get_state()
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
        self.assertEqual(pserver.state_map.state, candidate)
        self.logger.info("State did not change, test passed, cleaning up")
        pserver.clear_pause_on_substate(Substate.voting)
        pserver.resume()
        
    def test_b_msg_ignores(self):
        pserver = self.preamble()
        r_server = pserver.get_raftframe_server()
        # leave the timer off but allow comms 
        async def allow_msgs():
            await pserver.resume_new_messages()
        self.loop.run_until_complete(allow_msgs())
        client =  MemoryBankTellerClient("localhost", 5000)
        status = client.get_status()
        self.assertIsNotNone(status)
        tsm = RequestVoteMessage(("localhost", 5001),
                                 ("localhost", 5000),
                                 0,
                                 {}, 0, 0, 0)
        self.logger.info("Sending request vote message" \
                         " expecting no error ignore")
        client.direct_message(tsm)
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        tsm = AppendResponseMessage(("localhost", 5001),
                                    ("localhost", 5000),
                                    0,
                                    {}, 0, 0, 0)
        self.logger.info("Sending AppendEntries response message" \
                         " expecting no error ignore")
        client.direct_message(tsm)
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        tsm = HeartbeatResponseMessage(("localhost", 5001),
                                           ("localhost", 5000),
                                           0,
                                           {})
        self.logger.info("Sending heartbeat response message" \
                         " expecting no error ignore")
        client.direct_message(tsm)
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)

    def test_b_msg_rejects(self):
        pserver = self.preamble()
        r_server = pserver.get_raftframe_server()
        # leave the timer off but allow comms 
        async def allow_msgs():
            await pserver.resume_new_messages()
        self.loop.run_until_complete(allow_msgs())
        self.logger.info("Candidate paused, sending status query")
        client =  MemoryBankTellerClient("localhost", 5000)
        status = client.get_status()
        res = client.do_credit(10)
        self.assertTrue('not available' in res)
        self.logger.info("Status query returned 'not available' as expected")
        self.logger.info("Setting candidate to terminated")
        candidate = pserver.state_map.get_state()
        candidate.terminated = True
        self.assertTrue(candidate.is_terminated())
        # any old message would do
        hbm = HeartbeatMessage(("localhost", 5001),
                               ("localhost", 5000),
                               0,
                               {},0,0,0)
        self.logger.info("Sending heartbeat message expecting reject")
        client.direct_message(hbm)
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        start_time = time.time()
        while time.time() - start_time < 3:
            time.sleep(0.05)
            if len(r_server.unhandled_errors) > 0:
                break
        self.assertEqual(len(r_server.get_unhandled_errors()), 1)
        self.logger.info("Reject resulted in r_server saving error\n%s",
                         r_server.get_unhandled_errors()[0])
        self.logger.info("clearing terminated flag")
        candidate.terminated = False
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
            if len(r_server.unhandled_errors) > 1:
                break
        self.assertEqual(len(r_server.get_unhandled_errors()), 2)
        self.logger.info("Reject resulted in server saving error\n%s",
                         r_server.get_unhandled_errors()[1])
        
        self.logger.info("starting other two servers and waiting for election")
        pserver.clear_substate_pauses()
        self.cluster.servers[1].start()
        self.cluster.servers[2].start()
        pserver.resume()
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
        self.cluster.servers[0].stop()
        self.cluster.servers[1].stop()
        self.cluster.servers[2].stop()
            
    def test_b_heartbeat_resign(self):
        pserver = self.preamble()
        candidate = pserver.state_map.get_state()
        async def try_heartbeat():
            hbm = HeartbeatMessage(("localhost", 5001),
                                   ("localhost", 5000),
                                   0,
                                   {
                                       "leaderId": "server_1",
                                       "leaderPort": ("localhost", 5001),
                                       "entries": [],
                                   }, 0, 0, 0)

            await candidate.on_heartbeat(hbm)
            await asyncio.sleep(0.01)
        pserver.clear_pause_on_substate(Substate.voting)
        pserver.pause_on_substate(Substate.leader_lost)
        pserver.resume()
        self.loop.run_until_complete(try_heartbeat())
        self.assertFalse(pserver.state_map.get_state() == candidate)


    def release_to_resign(self, pserver, method):
        candidate = pserver.state_map.get_state()
        pserver.clear_pause_on_substate(Substate.voting)
        pserver.pause_on_substate(Substate.leader_lost)
        pserver.resume()
        self.loop.run_until_complete(method())
        async def wait_for_pause():
            start_time = time.time()
            while time.time() - start_time < timeout_basis * 2:
                await asyncio.sleep(0.001)
                if pserver.paused:
                    return
            raise Exception("timeout waiting for pause")
        self.loop.run_until_complete(wait_for_pause())
        pserver.clear_pause_on_substate(Substate.leader_lost)

    def test_b_got_append(self):
        pserver = self.preamble()
        candidate = pserver.state_map.get_state()
        async def try_append():
            aem = AppendEntriesMessage(("localhost", 5001),
                                       ("localhost", 5000),
                                       0,
                                       {
                                           "leaderId": "server_1",
                                           "leaderPort": ("localhost", 5001),
                                           "entries": [],
                                       }, 0, 0, 0)
            await candidate.on_append_entries(aem)
            await asyncio.sleep(0)
        self.release_to_resign(pserver, try_append)
        self.assertFalse(pserver.state_map.get_state() == candidate)
        pserver.resume()
        
    def test_b_on_append(self):
        pserver = self.preamble()
        candidate = pserver.state_map.get_state()
        async def try_append():
            aem = AppendEntriesMessage(("localhost", 5001),
                                       ("localhost", 5000),
                                       0,
                                       {
                                           "leaderId": "server_1",
                                           "leaderPort": ("localhost", 5001),
                                           "entries": [],
                                       }, 0, 0, 0)
            await candidate.on_append_entries(aem)
            await asyncio.sleep(0)
        self.release_to_resign(pserver, try_append)
        self.assertFalse(pserver.state_map.get_state() == candidate)
        pserver.resume()

        
    def test_vote_response_1(self):
        pserver = self.preamble()
        candidate = pserver.state_map.get_state()
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
        self.release_to_resign(pserver, try_vote_response)
        self.assertFalse(pserver.state_map.get_state() == candidate)
        pserver.resume()
            
    def test_vote_response_2(self):
        pserver = self.preamble()
        candidate = pserver.state_map.get_state() 
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
        self.release_to_resign(pserver, try_vote_response)
        self.assertFalse(pserver.state_map.get_state()  == candidate)
        pserver.resume()

    def test_vote_response_3(self):
        # two votes from the same sender should not cause a resign
        pserver = self.preamble()
        candidate = pserver.state_map.get_state()
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
        self.assertEqual(pserver.state_map.get_state(), candidate)
        pserver.resume()
        
    def test_c_resign_fail(self):
        # Make the resign blow up
        pserver = self.preamble()
        r_server = pserver.thread.server
        pserver.state_map.explode_state_change = True
        candidate = pserver.state_map.get_state()
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        async def try_resign():
            await candidate.resign()
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(try_resign())
        # state change should have failed
        self.assertEqual(pserver.state_map.get_state(), candidate)
        self.assertEqual(len(r_server.get_unhandled_errors(clear=True)), 1)
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        candidate.terminated = False
        candidate.candidate_timer.terminated = False
        pserver.state_map.explode_state_change = False
        pserver.resume()
        self.loop.run_until_complete(try_resign())
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        self.assertNotEqual(pserver.state_map.get_state(), candidate)

    def test_c_promote_fail(self):
        # Make the promition to leader blow up
        pserver = self.preamble()
        r_server = pserver.thread.server
        pserver.state_map.explode_state_change = True
        candidate = pserver.state_map.get_state()
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        pserver.pause_on_substate(Substate.voting)
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
        self.assertEqual(pserver.state_map.get_state(), candidate)
        self.assertEqual(len(r_server.get_unhandled_errors(clear=True)), 1)
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        pserver.state_map.explode_state_change = False
        candidate.candidate_timer.terminated = False
        candidate.all_votes = {}
        pserver.clear_pause_on_substate(Substate.voting)
        pserver.resume()
        self.loop.run_until_complete(try_vote_response())
        self.assertEqual(len(r_server.get_unhandled_errors()), 0)
        self.assertNotEqual(pserver.state_map.get_state(), candidate)
        self.loop.run_until_complete(asyncio.sleep(0.05))
        # let it do stop without errors
        pserver.state_map.get_state().heartbeat_timer.allow_failed_terminate = True



