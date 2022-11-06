from collections import defaultdict
import asyncio
import logging
from dataclasses import dataclass, field, asdict
import time
import traceback

from ..log.log_api import LogRec
from ..messages.append_entries import AppendEntriesMessage
from ..messages.request_vote import RequestVoteResponseMessage
from ..messages.command import ClientCommandResultMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from ..utils import task_logger
from .base_state import State, Substate, StateCode

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
        for other in self.server.other_nodes:
            # Assume follower is in sync, meaning we only send on new
            # log entry. If they are not, they will tell us that on
            # first heartbeat, and we will catch them up
            self.cursors[other] = FollowerCursor(other, last_index)
        self.heartbeat_timer = None
        self.last_hb_time = time.time()
        self.election_time = time.time()
        self.task = None

    def __str__(self):
        return "leader"

    def start(self):
        if self.terminated:
            raise Exception("cannot start a terminated state")
        self.heartbeat_timer = self.server.get_timer("leader-heartbeat",
                                                     self.log.get_term(),
                                                     self.heartbeat_timeout,
                                                     self.send_heartbeat)
        self.task = task_logger.create_task(self.on_start(),
                                            logger=self.logger,
                                            message="leader start method")
    async def stop(self):
        self.terminated = True
        if not self.heartbeat_timer.terminated:
            await self.heartbeat_timer.terminate()
        if self.task:
            self.task.cancel()
            await asyncio.sleep(0)
            
    async def on_start(self):
        if self.terminated:
            self.task = None
            return
        self.logger.debug("in on_start")
        self.heartbeat_timer.start()
        await self.send_heartbeat(first=True)
        self.logger.debug("changing substate to became_leader")
        await self.set_substate(Substate.became_leader)
        self.task = None
        
    def get_leader_addr(self):
        return self.server.endpoint
    
    async def on_heartbeat_response(self, message):
        self.heartbeat_logger.debug("got heartbeat response from %s",
                                    message.sender)
        addr = message.sender
        sender_index = message.data['last_index']
        if addr in self.cursors:
            cursor = self.cursors[addr]
        else:
            cursor = FollowerCursor(addr, sender_index)
            self.cursors[addr] = cursor
        cursor.last_heartbeat_index = sender_index
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
        if time.time() - self.last_hb_time >= self.heartbeat_timeout:
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
        cursor = self.cursors[message.sender]

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
        if local_commit is None:
            local_commit = -1
        if local_commit >= last_saved_index:
            # extra message, we good
            self.logger.debug("Follower %s voted to commit %s, "\
                              "ignoring since commit completed",
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
        self.logger.debug("after commit, commit_index = %s",
                          last_saved_index)
        # now broadcast a commit AppendEntries message
        await self.broadcast_commit(commit_rec)

        # Now see if there is a client to send the reply to
        # for the completed record. Since we committed it
        # we can now respond to client
        if commit_rec.context is None:
            return True
        reply_address = commit_rec.context.get('client_addr', None)
        if not reply_address:
            return True
        # This log record was for data submitted by client,
        # not an internal record such as term change.
        # Send as reply to client
        self.logger.debug("preparing reply for %s",
                          reply_address)
        reply = ClientCommandResultMessage(self.server.endpoint,
                                           reply_address,
                                           self.log.get_term(),
                                           commit_rec.user_data)
        self.logger.debug("sending reply message %s", reply)
        await self.server.post_message(reply)
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
                "prevLogIndex": prev_index,
                "prevLogTerm": prev_term,
                "entries": [],
                "leaderCommit": prev_index,
                "commitOnly": True
            }
        )
        self.logger.debug("(term %d) sending AppendEntries commit %d to all" \
                          " followers: %s",
                          self.log.get_term(), prev_index, message.data)
        await self.server.broadcast(message)
        
    async def send_append_entries(self, addr, start_index, end_index=None):
        # we need to make sure we don't starve heartbeat
        # if there is a big recovery in progress
        if time.time() - self.last_hb_time >= self.heartbeat_timeout:
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
        if end_index:
            for i in range(start_index + 1, end_index + 1):
                rec = self.log_read(i)
                entries.append(asdict(rec))
        message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            self.log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "prevLogIndex": prev_index,
                "prevLogTerm": prev_term,
                "entries": entries,
                "leaderCommit": self.log.get_commit_index(),
            }
        )
        cursor = self.cursors[addr]
        cursor.last_sent_index = entries[-1]['index']
        message._receiver = addr
        self.logger.debug("(term %d) sending AppendEntries " \
                          " %d entries to %s, first is %s",
                          self.log.get_term(),
                          len(entries), addr, start_index)
        await self.server.post_message(message)
        
    async def do_backdown(self, message):

        start_index = message.data['last_index'] + 1
        await self.send_append_entries(message.sender, start_index)
        
    async def send_heartbeat(self, first=False):
        elapsed =  time.time() - self.last_hb_time
        if elapsed > self.heartbeat_timeout + (self.heartbeat_timeout * 0.1):
            self.logger.info("slow heartbeat, expected %f.6f, got %f.6f",
                             self.heartbeat_timeout, elapsed)

        data = {
            "leaderId": self.server.name,
            "leaderPort": self.server.endpoint,
            "prevLogIndex": self.log.get_last_index(),
            "prevLogTerm": self.log.get_last_term(),
            "entries": [],
            "leaderCommit": self.log.get_commit_index(),
            }
        if first:
            data["first_in_term"] = True

        message = HeartbeatMessage(self.server.endpoint, None,
                                   self.log.get_term(), data)
        self.heartbeat_logger.debug("sending heartbeat to all commit = %s",
                                    message.data['leaderCommit'])
        await self.server.broadcast(message)
        self.heartbeat_logger.debug("sent heartbeat to all commit = %s",
                                    message.data['leaderCommit'])
        await self.set_substate(Substate.sent_heartbeat)
        self.last_hb_time = time.time()

    async def on_client_command(self, message):
        # we need to make sure we don't starve heartbeat
        # if there is a big recovery in progress
        if time.time() - self.last_hb_time >= self.heartbeat_timeout:
            await self.send_heartbeat()
        target = message.sender
        if message.original_sender:
            target = message.original_sender
        # call the user app 
        result = self.server.get_app().execute_command(message.data)
        if not result.log_response:
            # user app does not want to log response
            self.logger.debug("preparing no-log reply for %s",
                              target)
            reply = ClientCommandResultMessage(self.server.endpoint,
                                               target,
                                               self.log.get_term(),
                                               result.response)
            self.logger.debug("sending no-log reply message %s", reply)
            await self.server.post_message(reply)
            return True
        self.logger.debug("saving address for reply %s", target)

        # Before appending, get the index and term of the previous record,
        # this will tell the follower to check their log to make sure they
        # are up to date except for the new record(s)
        last_index = self.log.get_last_index()
        last_term = self.log.get_last_term()
        new_rec = LogRec(term=self.log.get_term(), user_data=result.response,
                         context=dict(client_addr=target))
        self.log.append([new_rec,])
        new_rec = self.log.read()
        update_message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            self.log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "prevLogIndex": last_index,
                "prevLogTerm": last_term,
                "entries": [asdict(new_rec),],
                "leaderCommit": self.log.get_commit_index(),
            }
        )
        self.logger.debug("(term %d) sending log update to all followers: %s",
                          self.log.get_term(), update_message.data)
        await self.server.broadcast(update_message)
        await self.set_substate(Substate.sent_new_entries)
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
        if time.time() - self.last_hb_time >= self.heartbeat_timeout:
            await self.send_heartbeat()
        self.logger.info("leader ignoring vote reply: message.term = %d local_term = %d",
                         message.term, self.log.get_term())
        return True

    async def on_vote_request(self, message): 
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
        self.logger.warning("leader unexpectedly got append entries from %s",
                            message.sender)
        return True
    
    async def on_term_start(self, message):
        self.logger.warning("leader got term start message from %s, makes no sense!",
                         message.sender) 
        return True

    async def on_heartbeat(self, message):
        self.logger.warning("Why am I getting hearbeat when I am leader?"\
                            "\n\t%s", message)
        return True


