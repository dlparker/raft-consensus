import unittest
import asyncio
import time
import traceback
import os

from raftframe.tests.common_tcase import TestCaseCommon

from raftframe.messages.heartbeat import HeartbeatMessage
from raftframe.states.base_state import StateCode
from dev_tools.pausing_app import PausingMonitor, PLeader

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2

class RejectingLeader(PLeader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rejecting = False
        self.respawn_count = 0
        self.orig = None
        
    def start(self):
        super().start()

    async def respawn_reset(self):
        if self.respawn_count == 0 and self.orig:
            print(f"returning to orig leader state instance")
            self.server.state_map.state = self.orig
            self.orig.heartbeat_timer.start()
            self.orig = None
        
    async def on_client_command(self, message):
        if message.code == "command" or self.orig:
            if self.rejecting:
                return False
            if self.respawn_count > 0:
                print(f"rejecting on respawn_count = {self.respawn_count}")
                # fool the server code into thinking we have changed
                # states but supplying a replacement
                new_state = RejectingLeader(self.server,
                                            self.heartbeat_timeout)
                
                self.server.state_map.state = new_state
                if self.orig:
                    new_state.orig = self.orig
                else:
                    new_state.orig = self
                    await self.heartbeat_timer.stop()
                new_state.respawn_count = self.respawn_count - 1
                if new_state.respawn_count == 0:
                    print("scheduling respawn reset")
                    asyncio.create_task(self.respawn_reset())
                return False
        if self.orig:
            self.server.state_map.state = self.orig
        res = await super().on_client_command(message)
        if self.orig:
            self.server.state_map.state = self
        return res
    
    def set_rejecting(self, value):
        self.rejecting = value

    def setup_respawn_reject(self, count):
        self.respawn_count = count

    async def stop(self):
        if not self.heartbeat_timer:
            return
        await super().stop()
        
class RejectingMonitor(PausingMonitor):

    def __init__(self, orig_monitor):
        super().__init__(orig_monitor.pbt_server,
                         orig_monitor.name,
                         orig_monitor.logger)
        self.state_map = orig_monitor.state_map
        self.state = orig_monitor.state
        self.substate = orig_monitor.substate
        self.pbt_server.state_map.remove_state_change_monitor(orig_monitor)
        self.pbt_server.state_map.add_state_change_monitor(self)
        
    async def new_state(self, state_map, old_state, new_state):
        new_state = await super().new_state(state_map, old_state, new_state)
        if new_state.code == StateCode.leader:
            self.state = RejectingLeader(new_state.server,
                                         new_state.heartbeat_timeout)
            return self.state
        return new_state
    
    
class TestEdges(TestCaseCommon):

    @classmethod
    def setUpClass(cls):
        super(TestEdges, cls).setUpClass()
        cls.total_nodes = 3
        cls.timeout_basis = 0.2
        if __name__.startswith("raftframe.tests"):
            cls.logger_name = __name__
        else:
            cls.logger_name = "raftframe.tests." + __name__

    def pre_start_callback(self):
        for spec in self.servers.values():
            spec.monitor = RejectingMonitor(spec.monitor)
    
    def test_simple_calls(self):
        self.preamble()
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        server = self.leader.server_obj
        # just check this here, make sure call is rejected
        with self.assertRaises(Exception) as context:
            stats = server.start()
        self.assertTrue("twice" in str(context.exception))
        self.assertEqual(self.leader.monitor.state, 
                         self.leader.server_obj.get_state())
        
    def test_reject_messages_while_changing_state(self):
        # The servers.server.py Server class has
        # logic to handle rejection of messages by
        # the current state object. The first step
        # is to see if the state object changed during
        # the call. This might happen if it was processing
        # a message as part of the election sequence.
        # If the state has not changed, it might still
        # be in the process of changing for such a reason.
        # This test ensures that the state_map reflects the
        # change in progress condition and also ensures that
        # the message will be rejected. The server logic
        # should wait a bit, then retry. We make it continue
        # to fail so that it will give up.
        # We target the leader server because it will
        # normally process client messages.
        self.preamble(pre_start_callback=self.pre_start_callback)
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        self.leader.monitor.state.set_rejecting(False)
        server = self.leader.server_obj
        estack = server.get_unhandled_errors()
        self.assertEqual(len(estack), 0)
        stats = client.do_log_stats()
        self.assertIsNotNone(stats)
        client.set_timeout(0.5)
        self.leader.pbt_server.state_map.changing = True
        self.leader.monitor.state.set_rejecting(True)
        # Server should try twice, then give up. Message
        # lost in limbo, so no reply sent
        with self.assertRaises(Exception) as context:
            stats = client.do_log_stats()
        self.assertTrue("timeout" in str(context.exception))
        # should save an error in the
        # "i don't know what to do with it" stack
        estack = server.get_unhandled_errors(clear=True)
        self.assertTrue(len(estack) > 0)

    def test_reject_messages_after_changing_state(self):
        # The servers.server.py Server class has
        # logic to handle rejection of messages by
        # the current state object. The first step
        # is to see if the state object changed during
        # the call. This might happen if it was processing
        # a message as part of the election sequence.
        # If the state has changed then the server should
        # retry with the new state. 
        # It should only retry once and if it still
        # gets rejected, it should save an error doohicky and
        # quit trying.
        # We make it do this by targeting the leader server
        # and setting up the state as changing and replacing
        # the leader object with another. This passes the equals
        # test in the server code, because it only wants to know
        # if the state object changed, not what kind it is.
        # We setup to reject a couple of attempts and then
        # set everything back to the original condition so that
        # the shutdown logic will work correctly and cleanup before
        # the next test. Look at the RejectingLeader above to
        # see lots of details about how that is done.
        # We expect a timeout because the message is never handled
        # so no reply. We also expect the server to record the error
        self.preamble(pre_start_callback=self.pre_start_callback)
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.set_timeout(0.5)
        server = self.leader.server_obj
        estack = server.get_unhandled_errors()
        self.assertEqual(len(estack), 0)
        self.leader.monitor.state.setup_respawn_reject(2)
        with self.assertRaises(Exception) as context:
            stats = client.do_log_stats()
        self.assertTrue("timeout" in str(context.exception))
        estack = server.get_unhandled_errors(clear=True)
        self.assertTrue(len(estack) > 0)
        






        
            
