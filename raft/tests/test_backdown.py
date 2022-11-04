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
from raft.messages.heartbeat import HeartbeatResponseMessage
from raft.messages.command import ClientCommandMessage
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
                                            base_port=5000,
                                            timeout_basis=timeout_basis,
                                            use_log_pull=False)
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
        self.logger.info("waiting for %s", label)
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
        
    def test_1(self):
        self.inner_1(all_committed=True)
        
    def inner_1(self, all_committed=True):
        self.servers = self.cluster.prepare()
        spec0 = self.servers["server_0"]
        spec1 = self.servers["server_1"]
        monitor0 = spec0.monitor
        monitor1 = spec1.monitor
        for spec in self.servers.values():
            spec.interceptor.add_trigger(InterceptorMode.out_after, 
                                       HeartbeatMessage._code)
            spec.interceptor.add_trigger(InterceptorMode.in_before, 
                                         HeartbeatMessage._code)
        self.cluster.start_one_server(spec0.name)
        self.cluster.start_one_server(spec1.name)
        self.logger.info("waiting for pause on election results")

        leader, follower = self.pause_waiter("election results")

        for spec in self.servers.values():
            spec.interceptor.clear_triggers()

        # let's get obnoxious level of debug logging
        follower.monitor.state.debug_dumper = False        

        # Setup pause for both when there is a new log entry
        # distributed, the first part of the commit dialog.
        # The leader will send a new entries message, so we setup
        # to stop when that is done. When the message gets to the
        # follower, it will process it and we will cause it to pause
        # after that step is done. In the code below we will
        # do a client simulation to send a client command that results
        # in a log record being written in the leader and starts
        # the commit dialog messages going
        leader.interceptor.add_trigger(InterceptorMode.out_after,
                                       AppendEntriesMessage._code)
        follower.interceptor.add_trigger(InterceptorMode.in_after,
                                         AppendEntriesMessage._code)
                                         
        self.cluster.resume_all_paused_servers()
        self.resume_waiter()

        # We want to do client things, but our pausing of
        # servers will cause normal client to timeout. To
        # get around that we use the memory_comms.py MemoryComms
        # class directly, and to do that we need to supply it with
        # a server instance. Fortunately that instance doesn't have
        # to do much in this case.
        class FakeServer:

            def __init__(self):
                self.in_queue = asyncio.Queue()
                
            async def on_message(self, message):
                # will be called by MemoryComms
                # when server replies to commands
                await self.in_queue.put(message)
        
        server_1 = FakeServer()
        comms = MemoryComms()
        my_addr = ('localhost', 6000)
        async def do_query():
            # Create the in memory queue based
            # client instance using the fake server
            # and the above address
            await comms.start(server_1, my_addr)
            # send the query command message, which
            # will be logged at the leader
            command = ClientCommandMessage(my_addr,
                                           leader.server_obj.endpoint,
                                           None,
                                           "query")
            await comms.post_message(command)
        self.loop.run_until_complete(do_query())

        # Wait for the servers to pause, at which point the first
        # step in the two phase log copy is done
        self.pause_waiter("update via append")
        # At this point AppendEntries message has run and follower should
        # have a record in its log.
        flog = follower.server_obj.get_log()
        frec = flog.read()
        self.assertIsNotNone(frec)

        # The next step will be a repeat of the first in terms
        # of the message dialog, just with changes in the content
        # of the messages. In this case the AppendEntries message
        # will not contain a new log record, rather it will command
        # a commit at the follower. 
        # clear the pause conditions so we can setup them up anew
        self.cluster.resume_all_paused_servers()
        # The next pause happens too quickly for the wait for resume
        # logic to be reliable, so we'll skip it, no self.resume_waiter().
        # Instead we'll just wait for the pause
        
        self.pause_waiter("commit via append")
        # At this point AppendEntries for commit has run and follower
        # should have commited the record
        frec = flog.read()
        self.assertIsNotNone(frec)
        self.assertTrue(frec.committed)

        # Clear the pause conditions and make new ones,
        # this time to catch the heartbeat dialog
        leader.interceptor.clear_triggers()
        follower.interceptor.clear_triggers()

        leader.interceptor.add_trigger(InterceptorMode.out_after,
                                       HeartbeatMessage._code)
        follower.interceptor.add_trigger(InterceptorMode.in_after,
                                         HeartbeatMessage._code)

        self.cluster.resume_all_paused_servers()
        self.pause_waiter("heartbeat")
        # By the time the servers pause at the end of the
        # first half of the heartbeat dialog, the leader
        # should have sent a reply to the client command, because
        # the end of the commit cycle includes this step.
        # We will wait for it to make sure all is ok.
        self.reply = None
        async def get_reply():
            start_time = time.time()
            while time.time() - start_time < 2:
                if not server_1.in_queue.empty():
                    self.reply = await server_1.in_queue.get()
                    break
        self.loop.run_until_complete(get_reply())
        self.assertIsNotNone(self.reply)

        # Now we want to trigger a pause when the heartbeat
        # sequence informs the follower that there are records
        # for it to fetch from the leader. We do this by inserting
        # some records into the leader's log, but not running
        # the leader's code that normally does this, so the
        # leader never sends an AppendEntries message for these
        # records. This is equivalent to what happens when a follower
        # restarts or reconnects after missing some messages. 

        # pause them before fiddling log
        leader.interceptor.add_trigger(InterceptorMode.out_after, 
                                       HeartbeatMessage._code)
        follower.interceptor.add_trigger(InterceptorMode.in_before, 
                                         HeartbeatMessage._code)
        self.pause_waiter("Arrival of heartbeat before leader log adds")
        
        llog = leader.server_obj.get_log()
        lrec = flog.read()
        llog.append([lrec,lrec])
        lrec = flog.read()
        llog.commit(2)

        # Next Follower heartbeat reply will indicate that it
        # is not up to date, so leader should send an
        # append entries to start the catch up
        leader.interceptor.add_trigger(InterceptorMode.out_before,
                                       AppendEntriesMessage._code)
        self.logger.debug("\n\n\tResuming after log fiddle\n\n")
        self.cluster.resume_all_paused_servers()
        self.pause_waiter("Arrival of heartbeat after log adds")
        msg = follower.interceptor.pausing_message
        self.assertEqual(msg.data['prevLogIndex'], 2)
        follower.interceptor.add_trigger(InterceptorMode.out_before,
                                         HeartbeatResponseMessage._code)
        
        async def resume(pbt_server):
            await pbt_server.resume_all()
        self.logger.debug("\n\n\tResuming follower only, should get heartbeat\n\n")
        self.loop.run_until_complete(resume(follower.pbt_server))
        self.pause_waiter("Before heartbeat response on update beginning")

        # Follower heartbeat reply will indicate that it
        # is not up to date, so leader should send an
        # append entries to start the catch up
        msg = follower.interceptor.pausing_message
        self.assertEqual(msg.data['last_index'], 0)
        
        follower.interceptor.clear_triggers()
        leader.interceptor.clear_triggers()
        
        leader.interceptor.add_trigger(InterceptorMode.out_after, 
                                       AppendEntriesMessage._code)
        follower.interceptor.add_trigger(InterceptorMode.in_before, 
                                         AppendEntriesMessage._code)

        self.logger.debug("\n\n\tResuming, looking for AppendEntries 1\n\n")

        # We need to make sure that the leader is already resumed
        # when the follower starts, because there is a race between
        # the append message going out and the resume. If the follower
        # gets far enough ahead, then the leader will be resumed after
        # it has already hit the interceptor. So we start them
        # manually to get the right order
        
        self.loop.run_until_complete(resume(leader.pbt_server))
        self.loop.run_until_complete(resume(follower.pbt_server))
        self.pause_waiter("AppendEntries-1 before")
        msg = follower.interceptor.pausing_message
        self.assertEqual(len(msg.data['entries']), 1)
        # before the append entries is applied, we should have only
        # the first record.
        before_rec = flog.read()
        self.assertEqual(before_rec.index, 0)

        # let it run and stop before the next append is applied
        self.logger.debug("\n\n\tResuming, looking for AppendEntries-1\n\n")
        self.loop.run_until_complete(resume(leader.pbt_server))
        self.loop.run_until_complete(resume(follower.pbt_server))
        self.pause_waiter("AppendEntries-2 before")
        mid_rec = flog.read()
        self.assertEqual(mid_rec.index, 1)
        msg = follower.interceptor.pausing_message
        self.assertEqual(len(msg.data['entries']), 1)

        
        follower.interceptor.clear_triggers()
        leader.interceptor.clear_triggers()
        follower.interceptor.add_trigger(InterceptorMode.in_after, 
                                           HeartbeatMessage._code)
        leader.interceptor.add_trigger(InterceptorMode.out_after, 
                                       HeartbeatMessage._code)
        self.logger.debug("\n\n\tResuming, looking for heartbeat\n\n")
        self.loop.run_until_complete(resume(leader.pbt_server))
        self.loop.run_until_complete(resume(follower.pbt_server))
        self.pause_waiter("Post append heartbeat")
                                     
        after_rec = flog.read()
        self.assertEqual(after_rec.index, 2)
        
        #follower.monitor.state.debug_dumper = False        
        # Clean it all up and the resume all so that
        # the shutdown code will run cleanly
        leader.interceptor.clear_triggers()
        follower.interceptor.clear_triggers()
        self.cluster.resume_all_paused_servers()
