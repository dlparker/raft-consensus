from collections import defaultdict
import asyncio
import logging
from dataclasses import dataclass, field, asdict
from typing import Union
import time
import traceback

from raftframe.app_api.app import CommandResult
from raftframe.log.log_api import LogRec, RecordCode
from raftframe.messages.append_entries import AppendEntriesMessage
from raftframe.messages.request_vote import RequestVoteResponseMessage
from raftframe.messages.command import ClientCommandResultMessage
from raftframe.messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from raftframe.utils import task_logger
from raftframe.states.base_state import State, Substate, StateCode

@dataclass
class FollowerCursor:
    addr: str
    last_sent_index: int = field(default=0)
    last_saved_index: int = field(default=0)
    last_commit: int = field(default=0)
    last_heartbeat_index: int = field(default=0)
    
class Leader(State):

    my_code = StateCode.leader
    
    def __init__(self, server, heartbeat_timeout=0.5):
        super().__init__(server, self.my_code)
        self.heartbeat_timeout = heartbeat_timeout
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        last_index = self.log.get_last_index()
        self.logger.info('Leader on %s in term %s', self.server.endpoint,
                         self.log.get_term())
        self.cursors = {}
        self.heartbeat_timer = None
        self.last_hb_time = time.time()
        self.election_time = time.time()
        self.start_task = None
        self.command_task = None
        self.command_result = None
        self.command_committed = False
        self.command_timeout_limit = 1.0

    def __str__(self):
        return "leader"

    def get_cursor(self, addr):
        if addr in self.cursors:
            cursor = self.cursors[addr]
        else:
            cursor = FollowerCursor(addr)
            self.cursors[addr] = cursor
        return cursor
        
    def start(self):
        if self.terminated:
            raise Exception("cannot start a terminated state")
        self.heartbeat_timer = self.server.get_timer("leader-heartbeat",
                                                     self.log.get_term(),
                                                     self.heartbeat_timeout,
                                                     self.send_heartbeat)
        self.start_task = task_logger.create_task(self.on_start(),
                                            logger=self.logger,
                                            message="leader start method")
    async def stop(self):
        self.terminated = True
        if not self.heartbeat_timer.terminated:
            await self.heartbeat_timer.terminate()
        if self.start_task:
            self.start_task.cancel()
            await asyncio.sleep(0)
            
    async def on_start(self):
        if self.terminated:
            self.start_task = None
            return
        self.logger.debug("in on_start")
        self.heartbeat_timer.start()
        await self.insert_term_start()
        self.logger.debug("changing substate to became_leader")
        await self.set_substate(Substate.became_leader)
        self.start_task = None
        
    def get_leader_addr(self):
        return self.server.endpoint

    async def insert_term_start(self):
        rec = LogRec(RecordCode.no_op,
                     term=self.log.get_term(),
                     committed=True,
                     user_data=dict(addr=self.server.endpoint,
                                    time=time.time()))
        last_index = self.log.get_last_index()
        last_term = self.log.get_last_term()
        self.log.append([rec,])
        start_rec = self.log.read()
        # we don't need consensus to commit
        self.log.commit(start_rec.index)
        update_message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            self.log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "entries": [asdict(start_rec),],
            },
            last_term, last_index, self.log.get_commit_index(),
        )
        self.logger.debug("(term %d) sending log update to all followers: %s",
                          self.log.get_term(), update_message.data)
        await self.server.broadcast(update_message)
        
    async def on_heartbeat_response(self, message):
        self.heartbeat_logger.debug("got heartbeat response from %s",
                                    message.sender)
        cursor = self.get_cursor(message.sender)
        sender_index = message.data['last_index']
        cursor.last_heartbeat_index = sender_index
        if not message.data['success']:
            if self.log.get_term() < message.term:
                self.logger.debug("Follower %s rejected append because" \
                                  " term there is %s but here only %s",
                                  message.sender, message.term,
                                  self.log.get_term())
                await self.resign()
                return True
        if sender_index < self.log.get_last_index():
            self.logger.debug("Sender %s needs catch up, "\
                              "Sender index %d, last_sent %d, "\
                              "local_last %d", message.sender,
                              sender_index, cursor.last_sent_index,
                              self.log.get_last_index())
            await self.do_backdown(message)
        return True
    
    async def on_append_response(self, message):
        # we need to make sure we don't starve heartbeat
        # if there is a big recovery in progress
        if (time.time() - self.last_hb_time
            >= self.heartbeat_timeout): # pragma: no cover overload
            await self.send_heartbeat()
        last_index = self.log.get_last_index()
        if last_index == 0:
            msg = "Got append response when log is empty"
            self.logger.warning(msg)
            self.server.record_unexpected_state(self, msg)
            return True
        if not message.data['success']:
            if self.log.get_term() < message.term:
                self.logger.debug("Follower %s rejected append because" \
                                  " term there is %s but here only %s",
                                  message.sender, message.term,
                                  self.log.get_term())
                await self.resign()
                return True
            # follower did not have the prev rec, so back down
            # by one and try again
            self.logger.debug("calling do_backdown for follower %s"\
                              "\nresponse = %s",
                              message.sender, message.data)
            await self.do_backdown(message)
            return True
        # must have been happy with the message. Now let's
        # see what it was about, a append or a commit
        cursor = self.get_cursor(message.sender)

        if message.data.get('commit_only', False):
            cursor.last_commit = message.leaderCommit
            self.logger.debug("Follower %s acknowledged commit %s",
                              message.sender, cursor.last_commit)
            return True
        # Follower will tell us last inserted record when
        # we sent new entries, so that means it wasn't just
        # a commit message.
        last_saved_index = message.data['last_entry_index']
        if last_saved_index > cursor.last_saved_index:
            # might have had out of order things cause
            # a resend
            cursor.last_saved_index = last_saved_index
        if self.log.get_last_index() > cursor.last_saved_index:
            self.logger.debug("Follower %s not up to date, "\
                              "follower index %s but leader %s, " \
                              "doing sending next",
                              message.sender, last_saved_index,
                              self.log.get_last_index())
            await self.send_append_entries(message.sender,
                                           cursor.last_saved_index + 1)
            return True
        
        if self.log.get_last_index() < last_saved_index:
            msg = f"Follower {message.sender} claims record "\
                f" {last_saved_index} but ours only go up to " \
                f" {self.log.get_last_index()} "
            self.logger.warning(msg)
            self.server.record_unexpected_state(self, msg)
            return True
            
        # If we got here, then follower is up to date with
        # our log. If we have committed the record, then
        # we have alread acheived a quorum so we can
        # ignore the follower message.
        # If we have not committed it, then we need to see
        # if we can by checking the votes already counted

        local_commit = self.log.get_commit_index()
        if local_commit >= last_saved_index:
            # This was not a commit only message, that is caught
            # above, so we just got the last response to a catch
            # up sequence
            self.logger.debug("Follower %s up to index %s, "\
                              " from catch up messages ",
                              message.sender, last_saved_index)
            return True
        # counting this node, so replies plus 1
        expected_confirms = (self.server.total_nodes - 1) / 2
        received_confirms = 0
        for cursor in self.cursors.values():
            if cursor.last_saved_index == last_saved_index:
                received_confirms += 1
        self.logger.debug("confirmation of log rec %d received from %s "\
                          "brings total to %d plus me out of %d",
                          last_saved_index, message.sender,
                          received_confirms, len(self.cursors) + 1)
        if received_confirms < expected_confirms:
            self.logger.debug("not enough votes to commit yet")
            return True
        # we have enough to commit, so do it
        self.log.commit(last_saved_index)
        commit_rec = self.log.read(last_saved_index)
        self.command_committed = True
        self.logger.debug("after commit, commit_index = %s, self.command_committed = %s",
                          last_saved_index, self.command_committed)
        # now broadcast a commit AppendEntries message
        await self.broadcast_commit(commit_rec)
        return True

    async def broadcast_commit(self, commit_rec):
        prev_index = commit_rec.index
        prev_term = commit_rec.term
        message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            self.log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "entries": [],
                "commitOnly": True
            },
            prev_term, prev_index, prev_index
        )
        self.logger.debug("(term %d) sending AppendEntries commit %d to all" \
                          " followers: %s",
                          self.log.get_term(), prev_index, message.data)
        await self.server.broadcast(message)
        
    async def send_append_entries(self, addr, start_index, send_multi=True):
        # we need to make sure we don't starve heartbeat
        # if there is a big recovery in progress
        if (time.time() - self.last_hb_time
            >= self.heartbeat_timeout): # pragma: no cover overload
            await self.send_heartbeat()
        entries = []
        rec = self.log.read(start_index)
        if start_index == 1:
            prev_index = 0
            prev_term = 0
        else:
            prev_rec = self.log.read(start_index - 1)
            prev_index = prev_rec.index
            prev_term = prev_rec.term
        entries.append(asdict(rec))
        if send_multi and start_index < self.log.get_last_index():
            up_to = min(self.log.get_last_index(), 10)
            for i in range(start_index + 1, up_to + 1):
                rec = self.log.read(i)
                entries.append(asdict(rec))
        message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            self.log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "entries": entries,
            },
            prev_term, prev_index, self.log.get_commit_index()
        )
        cursor = self.get_cursor(addr)
        cursor.last_sent_index = entries[-1]['index']
        message._receiver = addr
        self.logger.debug("(term %d) sending AppendEntries " \
                          " %d entries to %s, first is %s " \
                          " prev_term = %d prev_index = %d " ,
                          self.log.get_term(),
                          len(entries), addr, start_index,
                          prev_term, prev_index)
        await self.server.post_message(message)
        
    async def do_backdown(self, message):
        start_index = message.data['last_index'] + 1
        await self.send_append_entries(message.sender, start_index)
        
    async def send_heartbeat(self):
        data = {
            "leaderId": self.server.name,
            "leaderPort": self.server.endpoint,
            "entries": [],
            }

        message = HeartbeatMessage(self.server.endpoint, None,
                                   self.log.get_term(), data,
                                   self.log.get_last_term(),
                                   self.log.get_last_index(),
                                   self.log.get_commit_index())
                                   
        self.heartbeat_logger.debug("sending heartbeat to all term = %s" \
                                    " prev_index = %s" \
                                    " prev_term = %s" \
                                    " commit = %s",
                                    message.term,
                                    message.prevLogIndex,
                                    message.prevLogTerm,
                                    message.leaderCommit)
        await self.server.broadcast(message)
        self.heartbeat_logger.debug("sent heartbeat to all commit = %s",
                                    message.leaderCommit)
        await self.set_substate(Substate.sent_heartbeat)
        self.last_hb_time = time.time()

    async def on_client_command(self, message):
        target = message.sender
        if message.original_sender:
            # client sent request to some other server, which forwarded it
            # here.
            target = message.original_sender
        command = message.data
        
        loop = asyncio.get_running_loop()
        # if you don't do this, the wait loop in the command task blocks
        # any comms ops, don't know why. 
        loop.create_task(self.process_command(command, client=target))
        return True

    async def process_command(self, command, callback=None, client=None):
        # we need to make sure we don't starve heartbeat
        # if there is a big recovery in progress
        if (time.time() - self.last_hb_time
            >= self.heartbeat_timeout): # pragma: no cover overload
            await self.send_heartbeat()

        if self.command_task is not None:
            self.logger.info("Waiting for current command to finish")
            start_time = time.time()
            while self.command_task is not None:
                # whoever requested the command run should do the
                # timeout watch and clear it up, but just in case
                # that logic fails . . .
                if time.time() - start_time > self.command_timeout_limit * 1.1: # pragma: no cover error
                    raise Exception("timeout waiting for previous command to complete or timeout")
                await asyncio.sleep(0.001)
            self.logger.info("Finished waiting for command, starting new task")

        loop = asyncio.get_running_loop()
        self.command_task = loop.create_task(self.command_runner_wrapper(command, client, callback))

        start_time = time.time()
        while self.command_result is None:
            await asyncio.sleep(0.001)
            if (time.time() - self.last_hb_time
                >= self.heartbeat_timeout): # pragma: no cover overload
                await self.send_heartbeat()
            if time.time() - start_time > self.command_timeout_limit:
                self.logger.error("Timeout after one second running command %s", command)
                self.command_task.cancel()
                await self.command_task
                self.command_task = None
                if callback is not None:
                    result = CommandResult(command, None,
                                           error="Command cancelled because it took too long")
                    await callback(result)
                return False
        self.command_result = None
        self.command_task = None
        return True

    async def command_runner_wrapper(self, command, client=None, callback=None):
        try:
            await self.command_runner(command, client, callback)
        except asyncio.exceptions.CancelledError:  # pragma: no cover error
            msg = f"Command runner task cancelled, command was '{command}'"
            self.command_result = CommandResult(command, None, error=msg)
            self.logger.debug("Finished command runner, setting self.result to %s",
                              self.command_result)
            return
        except Exception:
            msg = f"Command runner task raised exception on command `{command}`\n"
            msg += f"{traceback.format_exc()}"
            self.command_result = CommandResult(command, None, error=msg)
            self.logger.debug("Finished command runner, setting self.result to %s",
                              self.command_result)
            if client:
                self.logger.debug("preparing error reply for %s",
                                  client)
                try:
                    reply = ClientCommandResultMessage(self.server.endpoint,
                                                       client,
                                                       self.log.get_term(),
                                                       data = None,
                                                       error=msg)
                    self.logger.debug("sending error reply message %s", reply)
                    await self.server.post_message(reply)
                except Exception: # pragma: no cover error
                    self.logger.error("could not send command failed reply to client")
            elif callback is not None:
                try:
                    await callback(self.command_result)
                except Exception: # pragma: no cover error
                    self.logger.error("callback on error got exception %s",
                                      traceback.format_exc())
            return
            
    async def command_runner(self, command, client=None, callback=None):
        self.command_committed = False
        self.command_result = None
        result = await self.server.get_app().execute_command(command)
        if not result.log_response:
            if client:
                self.logger.debug("preparing no-log reply for %s",
                                  client)
                reply = ClientCommandResultMessage(self.server.endpoint,
                                                   client,
                                                   self.log.get_term(),
                                                   result.response)
                self.logger.debug("sending no-log reply message %s", reply)
                await self.server.post_message(reply)
            elif callback is not None:
                await callback(result)
            self.command_result = result
            self.logger.debug("Finished command runner, setting self.result to %s", result)
            return True
        
        # Before appending, get the index and term of the previous record,
        # this will tell the follower to check their log to make sure they
        # are up to date except for the new record(s)
        last_index = self.log.get_last_index()
        last_term = self.log.get_last_term()
        # When the transaction is complete, meaning we receive acknowledge
        # from a quorum, we want to notify whatever code is waiting, locally
        # or at the client if this was a remove command call. So we
        # save enough info in the log record to be able to find the
        # notification target.
        new_rec = LogRec(term=self.log.get_term(),
                         user_data=result.response)
        self.log.append([new_rec,])
        new_rec = self.log.read()
        update_message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            self.log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "entries": [asdict(new_rec),],
            },
            last_term, last_index, self.log.get_commit_index()
        )
        self.logger.debug("(term %d) sending log update to all followers: %s",
                          self.log.get_term(), update_message.data)
        await self.server.broadcast(update_message)
        await self.set_substate(Substate.sent_new_entries)
        self.logger.debug("(term %d) waiting for commit on record %d, committed = %s",
                          self.log.get_term(), new_rec.index, self.command_committed)
        while not self.command_committed:
            await asyncio.sleep(0.01)
        self.command_committed = False
        self.logger.debug("\n\n\t(term %d) got commit on record %d\n\n",
                          self.log.get_term(), new_rec.index)

        # If we have a callback call it with the result
        if callback:
            await callback(result)
        # If there is a client to send the reply to
        # for the completed record. Since we committed it
        # we can now respond to client
        if client:
            self.logger.debug("preparing reply for %s",
                              client)
            reply = ClientCommandResultMessage(self.server.endpoint,
                                               client,
                                               self.log.get_term(),
                                               result.response)
            self.logger.debug("sending reply message %s", reply)
            await self.server.post_message(reply)
        self.logger.debug("Finished command runner, setting self.result to %s", result)
        self.command_result = result
        return True
    
    async def resign(self):
        if self.terminated:
            # order in async makes race for server states
            # switch and new timer fire
            return
        try:
            sm = self.server.get_state_map()
            sm.start_state_change("leader", "follower")
            self.terminated = True
            await self.heartbeat_timer.terminate() # never run again
            follower = await sm.switch_to_follower(self)
            self.logger.info("leader resigned")
            await self.stop()
        except:
            sm.failed_state_change("leader", "follower",
                                   traceback.format_exc())
            
    async def on_vote_received(self, message):
        # we need to make sure we don't starve heartbeat
        # if there is a big recovery in progress
        if (time.time() - self.last_hb_time
            >= self.heartbeat_timeout): # pragma: no cover overload
            await self.send_heartbeat()
        self.logger.info("leader ignoring vote reply: message.term = %d local_term = %d",
                         message.term, self.log.get_term())
        return True

    async def on_vote_request(self, message): 
        if (time.time() - self.last_hb_time
            >= self.heartbeat_timeout): # pragma: no cover overload
            await self.send_heartbeat()
        self.logger.info("vote request from %s, sending am leader",
                         message.sender)
        reply = RequestVoteResponseMessage(self.server.endpoint,
                                           message.sender,
                                           self.log.get_term(),
                                           {
                                               "already_leader":
                                               self.server.endpoint,
                                               "response": False
                                           })

        await self.server.post_message(reply)
        return True
    
    async def on_append_entries(self, message):
        if (time.time() - self.last_hb_time
            >= self.heartbeat_timeout): # pragma: no cover overload
            await self.send_heartbeat()
        self.logger.warning("leader unexpectedly got append entries from %s",
                            message.sender)
        if message.term > self.log.get_term():
            self.logger.info("new leader has taken over, resigning")
            await self.resign()
            return False # should make follower handle it
        return True
    
    async def on_heartbeat(self, message):
        if (time.time() - self.last_hb_time
            >= self.heartbeat_timeout): # pragma: no cover overload
            await self.send_heartbeat()
        if message.term > self.log.get_term():
            self.logger.info("new leader has taken over, resigning")
            await self.resign()
            return False # should make follower handle it
        self.logger.warning("Bogus leadership claim \n\t%s", message)
        return True

    
