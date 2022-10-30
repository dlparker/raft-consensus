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
        
    def test_1(self):
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
        
        llog = leader.server_obj.get_log()
        lrec = flog.read()
        llog.append([lrec,lrec])
        lrec = flog.read()
        # We commit only the first one as this is a useful
        # pattern compared to having both commited, so the
        # follower will insert but not commit the second one.
        llog.commit(1)
        leader.interceptor.clear_triggers()
        follower.interceptor.clear_triggers()

        # We don't know how many Heartbeats will take place
        # before the new log entries are noticed, might be zero
        # or one, depending on how the various async coroutines
        # end up getting run. So we need to do some fancy
        # footwork here.
        # The idea is the write a custom interceptor that looks
        # at the heartbeat message to see if the leaderCommit value
        # is 1, which indicates that the new record has been noticed
        # by the heartbeat generation code.
        # In the case of the leader we can just look for that after
        # sending the heartbeat and pause.
        # The follower is tricker, it has to check before the
        # heartbeat is processed, but we want to pause after processing.
        # So, we check for the condition and add a new interceptor
        # trigger to catch the end of the same message processing sequence.

        self.leader_got_pause = False
        async def leader_checker(mode, code, message):
            if message.data['leaderCommit'] == 1:
                self.leader_got_pause = True
                self.logger.info("leader pausing at new commit")
                await leader.pbt_server.pause_all(TriggerType.interceptor,
                                                  dict(mode=mode,
                                                       code=code))
                return True

        leader.interceptor.add_trigger(InterceptorMode.out_after,
                                       HeartbeatMessage._code,
                                       leader_checker)

        async def follower_checker(mode, code, message):
            if self.leader_got_pause:
                # add a trigger that will pause at the end of this
                # processing sequence
                self.logger.info("follower pausing at new commit")
                follower.monitor.state.debug_dumper = True
                follower.interceptor.add_trigger(InterceptorMode.in_after,
                                                 HeartbeatMessage._code)

                return True

        follower.interceptor.add_trigger(InterceptorMode.in_before, 
                                         HeartbeatMessage._code,
                                         follower_checker)

        self.cluster.resume_all_paused_servers()
        self.pause_waiter("unsync noticed")

        # Follower should have sent log pull request
        # so setup follower to pause on response from leader
        # and received by follower
        # leader gets stopped before next heartbeat
        follower.interceptor.clear_triggers()
        leader.interceptor.clear_triggers()
        
        leader.interceptor.add_trigger(InterceptorMode.out_before, 
                                         HeartbeatMessage._code)
        follower.interceptor.add_trigger(InterceptorMode.in_before, 
                                         LogPullResponseMessage._code)
        follower.interceptor.add_trigger(InterceptorMode.in_after,
                                         LogPullResponseMessage._code)

        self.cluster.resume_all_paused_servers()
        self.pause_waiter("log pull response before")

        before_rec = flog.read()

        follower.interceptor.clear_trigger(InterceptorMode.in_before, 
                                         LogPullResponseMessage._code)
        async def resume(pbt_server):
            await pbt_server.resume_all()
        self.loop.run_until_complete(resume(follower.pbt_server))
        self.pause_waiter("log pull response after")
                                     
        after_rec = flog.read()
        self.assertEqual(after_rec.index, before_rec.index + 2)
        self.assertFalse(after_rec.committed)
        
        #follower.monitor.state.debug_dumper = False        
        # Clean it all up and the resume all so that
        # the shutdown code will run cleanly
        leader.interceptor.clear_triggers()
        follower.interceptor.clear_triggers()
        self.cluster.resume_all_paused_servers()

    def atest_2(self):
        # The follower code allows for the leader to
        # send a AppendEntries message containing one or
        # more records that are already committed. This
        # does not currently happen because the follower
        # detects the possibility of such a case and calls
        # log_pull. The leader does not currently do the
        # raft sequence of backing down the log records until
        # it finds the first one that the client will except.
        # At some time in the future we may add a switch that
        # gets rid of the log_pull sequence and uses the
        # backdown sequence instead, so we want to keep the
        # relevant follower code alive. So it should work,
        # let's find out.
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

        self.cluster.resume_all_paused_servers()
        client = leader.get_client()
        client.do_query()
        client.do_query()
        client.do_query()

        # now the log on both should have three entries, all
        # committed
        leader.interceptor.add_trigger(InterceptorMode.out_after,
                                         HeartbeatMessage._code)
        follower.interceptor.add_trigger(InterceptorMode.in_after,
                                         HeartbeatMessage._code)

        self.pause_waiter("hearbeat on 3")

        # now delete the last records from the follower
        # log and make pretend to send AppendEntries from the
        # leader with the last entrie included. It will have
        # the committed flag set, so the follower should commit
        # the record.
        # Catch the follower after it sends the AppendEntriesResponse
        # and catch the leader after it processes the message.

        leader.interceptor.clear_triggers()
        follower.interceptor.clear_triggers()
        
        leader.interceptor.add_trigger(InterceptorMode.in_after,
                                       AppendEntriesResponse._code)
        follower.interceptor.add_trigger(InterceptorMode.out_after,
                                         AppendEntriesResponse._code)

        llog = leader.server_obj.get_log()
        lrec = flog.read()
        self.assertEqual(lrec.index, 2)
        flog.trim_after(1)
        
        self.pause_waiter("hearbeat on 3")
