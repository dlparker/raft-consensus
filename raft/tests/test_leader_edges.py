import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.messages.append_entries import AppendResponseMessage
from raft.messages.log_pull import LogPullMessage
from raft.comms.memory_comms import MemoryComms
from raft.log.log_api import LogRec
from raft.states.base_state import Substate
from raft.dev_tools.ps_cluster import PausingServerCluster
from raft.dev_tools.pausing_app import PausingMonitor, PCandidate, PLeader

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2

class BranchTricksLeader(PLeader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.terminate_on_start = False
        self.break_on_term_start = False
        self.broken = False

    async def on_start(self):
        self.terminated = self.terminate_on_start
        await super().on_start()

    async def send_term_start(self):
        if self.break_on_term_start:
            self.broken = True
            raise Exception('faking term start send error')
        await super().send_term_start()

class BranchTricksCandidate(PCandidate):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def start(self):
        self.server.get_state_map().switch_to_leader(self)


class BranchTricksMonitor(PausingMonitor):

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
        if new_state._type == "candidate":
            new_state = BranchTricksCandidate(new_state.server,
                                         new_state.timeout)
            return new_state
        if new_state._type == "leader":
            new_state = BranchTricksLeader(new_state.server,
                                         new_state.heartbeat_timeout)
            self.leader = new_state
            return new_state
        new_state = await super().new_state(state_map, old_state, new_state)
        return new_state
    
class TestTerminated(unittest.TestCase):

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
        # start two servers and wait for election to complete
        spec0 = servers["server_0"]
        monitor0 = spec0.monitor
        spec1 = servers["server_1"]
        monitor1 = spec1.monitor
        monitor0.set_pause_on_substate(Substate.joined)
        monitor0.set_pause_on_substate(Substate.sent_term_start)
        monitor1.set_pause_on_substate(Substate.joined)
        monitor1.set_pause_on_substate(Substate.sent_term_start)
        self.cluster.start_one_server(spec0.name)
        self.cluster.start_one_server(spec1.name)
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
            for spec in [spec0, spec1]:
                monitor = spec.monitor
                if monitor.state is not None:
                    if str(monitor.state) == "leader":
                        leader = spec
                        if spec.pbt_server.paused:
                            leader_paused = True
                    elif str(monitor.state) == "follower":
                        follower = spec
                        if spec.pbt_server.paused:
                            follower_paused = True
            if leader_paused and follower_paused:
                break
        self.assertTrue(leader_paused)
        self.assertTrue(follower_paused)
        return leader, follower
        
    def test_a_terminated_blocks(self):
        leader_spec, follower_spec = self.preamble()
        monitor = leader_spec.monitor
        leader = monitor.state
        # just make sure address returned makes sense
        self.assertEqual(leader_spec.addr, leader.get_leader_addr())
        self.logger.info("Leader paused, going to call stop on it")
        async def try_stop():
            await leader.stop()
        self.loop.run_until_complete(try_stop())
        self.logger.info("Terminating Leader, calling start should raise")
        self.assertTrue(leader.terminated)
        with self.assertRaises(Exception) as context:
            leader.start()
        self.assertTrue("terminated" in str(context.exception))
        leader_spec.monitor.clear_pause_on_substate(Substate.sent_term_start)
        leader_spec.monitor.clear_pause_on_substate(Substate.joined)
        follower_spec.monitor.clear_pause_on_substate(Substate.sent_term_start)
        follower_spec.monitor.clear_pause_on_substate(Substate.joined)
        self.loop.run_until_complete(leader_spec.pbt_server.resume_all())
        self.loop.run_until_complete(follower_spec.pbt_server.resume_all())


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
        # start just the one server and wait for it
        # to pause in candidate state
        spec = servers["server_0"]
        spec.monitor = BranchTricksMonitor(spec.monitor)
        monitor = spec.monitor
        monitor.set_pause_on_substate(Substate.became_leader)
        self.cluster.start_one_server("server_0")
        self.logger.info("waiting for switch to leader")
        start_time = time.time()
        while time.time() - start_time < 3 and not monitor.pbt_server.paused:
            time.sleep(0.05)
        self.assertTrue(monitor.pbt_server.paused)
        return spec

    def test_quick_stop(self):
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
        leader = BranchTricksLeader(spec.server_obj)
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
        self.assertIsNone(leader.task)
        self.assertEqual(leader.substate, Substate.starting)
        self.assertFalse(leader.heartbeat_timer.keep_running)

        # make sure that a failure to send term start messages
        # results in an error report to the server level
        leader.terminate_on_start = False
        server = spec.server_obj
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        leader.break_on_term_start = True
        leader.terminated = False
        async def do_start():
            leader.start()
        self.loop.run_until_complete(do_start())
        time.sleep(0.01)
        self.assertTrue(leader.broken)
        self.assertEqual(len(server.get_unhandled_errors()), 1)
        
    def atest_running(self):
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
        follower = monitor.state
        follower.terminated = True
        follower.leaderless_timer.terminated = True
        server = spec.server_obj
        self.assertEqual(len(server.get_unhandled_errors()), 0)
        leader = PLeader(spec.server_obj)
        sm = server.get_state_map()
        sm.state = leader
        monitor.set_pause_on_substate(Substate.sent_term_start)
        self.loop.run_until_complete(spec.pbt_server.resume_all())
        async def do_start():
            leader.start()
        self.loop.run_until_complete(do_start())
        while time.time() - start_time < 3 and not monitor.pbt_server.paused:
            time.sleep(0.05)
        self.assertTrue(monitor.pbt_server.paused)
        
    def brokentest_bad_appends(self):
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
        follower = monitor.state
        follower.terminated = True
        follower.leaderless_timer.terminated = True
        server = spec.server_obj
        self.assertEqual(len(server.get_handled_errors()), 0)
        leader = PLeader(spec.server_obj)
        sm = server.get_state_map()
        sm.state = leader
        log = server.get_log()
        term = log.get_term()
        async def do_strange_arm1():
            resp = AppendResponseMessage((0, 1),
                                         (0,0),
                                         term,
                                         {})
            await leader.on_append_response(resp)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(do_strange_arm1())
        self.assertEqual(len(server.get_handled_errors()), 1)
        err = server.get_handled_errors(clear=True)[0]
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
                                             "leaderCommit": None,
                                             "prevLogIndex": 2
                                         })
            await leader.on_append_response(resp)
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(do_strange_arm2())
        self.assertEqual(len(server.get_handled_errors()), 1)
        err = server.get_handled_errors(clear=True)[0]
        self.assertTrue("one less" in err['details'])
        
        # Send a log pull and capture the result to make sure it is
        # right
        # add more records, that makes 4
        log.append([new_rec,new_rec])
        class FakeServer:
            
            def __init__(self):
                self.in_queue = asyncio.Queue()
                
            async def on_message(self, message):
                await self.in_queue.put(message)
        
        server_1 = FakeServer()
        comms = MemoryComms()
        async def do_log_pull():
            # use non for start index, to trigger that
            # tiny branch
            async def start_comms():
                await comms.start(server_1, (0,0))
            message = LogPullMessage((0,0),
                                     (0.1),
                                     term,
                                     {
                                         "start_index": None,
                                         "leaderCommit": None,
                                         "prevLogIndex": None,
                                     })
            await leader.on_log_pull(message)
            await asyncio.sleep(0.01)
            msg = await server_1.in_queue.get()
            return msg
        msg = self.loop.run_until_complete(do_log_pull())
        
        


        

