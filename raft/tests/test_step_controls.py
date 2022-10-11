import unittest
import asyncio
import time
import logging
import traceback
import os
from dataclasses import dataclass

from raft.states.base_state import Substate
from raft.tests.setup_utils import Cluster
from raft.tests.bt_client import MemoryBankTellerClient
from raft.tests.timer import get_timer_set

#LOGGING_TYPE = "devel_one_proc" when using Mem comms and thread based servers
#LOGGING_TYPE = "devel_mp" when using UDP comms and MP process based servers
#LOGGING_TYPE = "silent" for no log at all
LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")

@dataclass
class RunData:
    leader: dict
    leader_addr: tuple
    first_follower: dict
    second_follower: dict

if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc" 

class TestStepControls(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = None
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.pause = True
        pass

    def tearDown(self):
        pass
    
    def get_client(self, port):
        return MemoryBankTellerClient("localhost", port)
        
    def run_data_from_status(self, status):
        run_data = {}
        leader_addr = status.data['leader']
        leader = None
        first_follower = None
        second_follower = None
        for name,sdef in self.cluster.server_recs.items():
            if sdef['port'] == leader_addr[1]:
                sdef['role'] = "leader"
                leader = sdef
            else:
                if not first_follower:
                    first_follower = sdef
                else:
                    second_follower = sdef
                sdef['role'] = "follower"
        self.logger.info("found leader %s", leader_addr)
        run_data = RunData(leader, leader_addr, first_follower, second_follower)
        return run_data

    async def wait_for_election_done(self, client, old_leader=None, timeout=5):
        self.logger.info("waiting for election results")
        start_time = time.time()
        while time.time() - start_time < timeout:
            await asyncio.sleep(0.01)
            status = await client.a_get_status()
            if status and status.data['leader']:
                new_leader_addr = status.data['leader']
                if old_leader:
                    if old_leader != new_leader_addr:
                        break
                else:
                    break
            status = None
            
        self.assertIsNotNone(status)
        self.assertIsNotNone(status.data['leader'])
        return self.run_data_from_status(status)
    
    def test_pause_at_first_election(self):
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        passes = 1
        for i in range(passes):
            print(f"pass {i}")
            self.loop.run_until_complete(self.inner_test_pause_at_first_election())
        
    async def inner_test_pause_at_first_election(self):
        self.cluster = Cluster(server_count=3,
                               use_processes=False,
                               logging_type=LOGGING_TYPE,
                               base_port=5000)
        if self.logger is None:
            self.logger = logging.getLogger(__name__)
            
        self.pause = True
        self.pause_states = {}
        self.pause_states[5000] = None
        self.pause_states[5001] = None
        self.pause_states[5002] = None
        self.follower_pause_count = 0
        
        async def pause_before(state, message):
            self.logger.info("pause_before %s %s", state, message)
            await get_timer_set().pause_all_this_thread()
            my_id = state.server.endpoint
            self.pause_states[my_id[1]] = dict(state=state, mode="before",
                                               message=message)
            while self.pause:
                await asyncio.sleep(0.01)
            await get_timer_set().resume_all_this_thread()
            self.pause_states[my_id[1]] = None
            return True
        
        async def pause_after(state, message):
            self.logger.debug("pause_after %s %s", state, message)
            await get_timer_set().pause_all_this_thread()
            my_id = state.server.endpoint
            self.pause_states[my_id[1]] = dict(state=state, mode="after",
                                               message=message)
            while self.pause:
                await asyncio.sleep(0.01)
            await get_timer_set().resume_all_this_thread()
            self.pause_states[my_id[1]] = None
            return True

        async def pause_on_substate(state, substate):
            self.logger.debug("pause_on %s %s", state, substate)
            await get_timer_set().pause_all_this_thread()
            my_id = state.server.endpoint
            self.pause_states[my_id[1]] = dict(state=state, mode="on_substate",
                                               substate=substate)
            if str(state)  == "follower":
                await state.leaderless_timer.stop()
                self.follower_pause_count += 1
                if self.follower_pause_count == 2:
                    await get_timer_set().pause_all()
            elif str(state)  == "candidate":
                await state.candidate_timer.stop()
            elif str(state)  == "leader":
                await state.heartbeat_timer.stop()
                comms = state.server.comms
                start_time = time.time()
                while time.time() - start_time < 1:
                    if comms.are_out_queues_empty():
                        break
                    await asyncio.sleep(0.25)
                if not comms.are_out_queues_empty():
                    raise Exception('leader comm queues never emptied')
            while self.pause:
                await asyncio.sleep(0.01)
            self.logger.debug("pause_on %s %s returning", state, substate)
            await get_timer_set().resume_all_this_thread()
            self.pause_states[my_id[1]] = None
            return True
            
        self.cluster.prep_mem_servers()
        for name,rec in self.cluster.server_recs.items():
            mserver = rec['memserver']
            mserver.state_map.set_pause_on_substate('all', Substate.joined,
                                                    pause_on_substate)
            mserver.state_map.set_pause_on_substate('all', Substate.new_leader,
                                                    pause_on_substate)
            
        self.cluster.start_all_servers()

        found_paused = 0
        start_time = time.time()
        while found_paused < 2 and time.time() - start_time < 5:
            found_paused = 0
            for port, rec in self.pause_states.items():
                if rec is not None:
                    found_paused += 1
            await asyncio.sleep(.01)
        self.assertEqual(found_paused, 2)
        # give it a bit more to catch both
        from pprint import pprint
        #pprint(self.pause_states)
        for port, rec in self.pause_states.items():
            if rec is None:
                continue
            self.assertEqual(rec['mode'], "on_substate")

        # the pause methods return true, which should clear the
        # pause settings
        self.pause = False
        pending = 2
        while pending:
            await asyncio.sleep(0.01)
            pending = 0
            for port in self.pause_states.keys():
                if self.pause_states[port] is not None:
                    pending += 1
        get_timer_set().resume_all()
        for name,rec in self.cluster.server_recs.items():
            mserver = rec['memserver']
            mserver.state_map.clear_pauses()
        await asyncio.sleep(0.01)
            
        client1 =  self.get_client(5000)
        run_data = await self.wait_for_election_done(client1)
 
        for name,rec in self.cluster.server_recs.items():
            mserver = rec['memserver']
            mserver.state_map.clear_pauses()

        self.pause = True
        # now get them all to pause after the next message send, which will
        # be a heartbeat
        for name,rec in self.cluster.server_recs.items():
            mserver = rec['memserver']
            mserver.state_map.set_pause_after_on_message('all', pause_after)

        # wait for heartbeat
        found_paused = 0
        start_time = time.time()
        while found_paused < 3 and time.time() - start_time < 3:
            found_paused = 0
            for port, rec in self.pause_states.items():
                if rec is not None:
                    found_paused += 1
            await asyncio.sleep(.01)
        # expecting both followers to pause after on_message for heartbeat
        # and leader for heartbeat response sent before the pause
        if found_paused < 3:
            from pprint import pprint
            pprint(self.pause_states)
            await get_timer_set().pause_all()
            await asyncio.sleep(1)
            pprint(self.pause_states)
            breakpoint()
            
        self.assertEqual(found_paused, 3)
        await get_timer_set().pause_all()
        await asyncio.sleep(.01)
        from pprint import pprint
        #pprint(self.pause_states)
        # all should be paused on after
        for port, rec in self.pause_states.items():
            if rec is None:
                continue
            self.assertEqual(rec['mode'], "after")

        self.pause = False
        await asyncio.sleep(0.01)
        for port in self.pause_states.keys():
            self.pause_states[port] = None
        get_timer_set().resume_all()
            
        self.cluster.stop_all_servers()
        await asyncio.sleep(0.5)
            
