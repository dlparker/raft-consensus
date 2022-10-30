from collections import defaultdict
import asyncio
import logging
from dataclasses import dataclass, field, asdict
import traceback

from ..log.log_api import LogRec
from ..messages.append_entries import AppendEntriesMessage
from ..messages.command import ClientCommandResultMessage
from ..messages.request_vote import RequestVoteResponseMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from ..messages.termstart import TermStartMessage
from ..messages.log_pull import LogPullResponseMessage
from ..utils import task_logger
from .base_state import State, Substate

@dataclass
class FollowerCursor:
    addr: str
    next_index: int
    last_index: int = field(default=-1)
    last_commit: int = field(default=-1)
    last_heartbeat_response_index: int = field(default=-1)

class Leader(State):

    my_type = "leader"
    
    def __init__(self, server, heartbeat_timeout=0.5):
        super().__init__(server, self.my_type)
        self.heartbeat_timeout = heartbeat_timeout
        self.use_log_pull = True
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index + 1
        else:
            # no log records yet
            last_index = -1
        self.logger.info('Leader on %s in term %s', self.server.endpoint,
                         log.get_term())
        self.followers = {}
        for other in self.server.other_nodes:
            # Assume follower is in sync, meaning we only send on new
            # log entry. If they are not, they will tell us that on
            # first heartbeat, and we will catch them up
            self.followers[other] = FollowerCursor(other, last_index)
        self.heartbeat_timer = None
        self.task = None

    def __str__(self):
        return "leader"

    def start(self):
        if self.terminated:
            raise Exception("cannot start a terminated state")
        log = self.server.get_log()
        self.heartbeat_timer = self.server.get_timer("leader-heartbeat",
                                                     log.get_term(),
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
        try:
            await self.send_term_start()
        except:
            sm = self.server.get_state_map()
            self.task = None
            self.server.record_failed_state_operation(self, "send_term_start",
                                                      traceback.format_exc())
            raise
        self.logger.debug("changing substate to became_leader")
        await self.set_substate(Substate.became_leader)
        self.task = None
        
    def get_leader_addr(self):
        return self.server.endpoint
    
    async def on_heartbeat_response(self, message):
        addr = message.sender
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec is None:
            local_index = -1
        else:
            local_index = last_rec.index
        if addr in self.followers:
            fc = self.followers[addr]
        else:
            fc = FollowerCursor(addr, local_index)
            self.followers[addr] = fc
        caller_index = message.data['last_index']
        fc.last_heartbeat_response_index = caller_index
        self.heartbeat_logger.debug("got heartbeat response from %s",
                                    message.sender)
        if not self.use_log_pull and las
            if caller_index < local_index:
                await self.do_backdown(addr, local_index)
        return True
    
    async def on_append_response(self, message):
        log = self.server.get_log()
        last_rec = log.read()
        if not last_rec:
            msg = "Got append response when log is empty"
            self.logger.warning(msg)
            self.server.record_unexpected_state(self, msg)
            return True
        if not message.data['response']:
            if self.use_log_pull:
                self.logger.debug("follower %s rejected append, expecting " \
                                  " log_pull to follow",
                                  message.sender)
                return True
            reason = message.data.get("reject_reason", None)
            self.logger.debug("follower %s rejected append for reason %s",
                              message.sender, reason)
            if reason != "no_match":
                return True
            last_sent_rec = message.data['prevLogIndex']
            # follower did not have the prev rec, so back down
            # by one and try again
            self.logger.debug("calling do_backdown for follower %s",
                              message.sender)
            await self.do_backdown(message.sender,
                                   message.data['leaderCommit'])
            return True
        # must have been happy with the message. Now let's
        # see what it was about, a append or a commit
        cursor = self.followers[message.sender]

        # Follower will tell us last inserted record when
        # we sent new entries, so that means it wasn't just
        # a commit message. If 'last_entry_index' is None,
        # then it was just a commit.
        last_sent_index = message.data['last_entry_index']
        cursor.last_index = last_sent_index
        if not last_sent_index:
            # message was commit only
            cursor.last_commit = message.data['leaderCommit']
            self.logger.debug("Follower %s acknowledged commit %s",
                              message.sender, cursor.last_commit)
            return True
            
        if last_rec.index > last_sent_index:
            self.logger.debug("Follower %s not up to date, "\
                              "follower index %s but leader %s, " \
                              "doing sending next",
                              message.sender, last_sent_index,
                              last_rec.index)
            await self.send_append_entries(message.sender,
                                           last_sent_index + 1)
            return True
        
        # If we got here, then follower is up to date with
        # our log. If we have committed the record, then
        # we have alread acheived a quorum and committed
        # the record. If that is true, then
        # we just want to ignore the message.
        # If we have not committed it, then we need to see
        # if we can by checking the votes already counted
        
        if log.get_commit_index() >= last_sent_index:
            # extra, we good
            self.logger.debug("Follower %s voted to commit %s, "\
                              "ignoring since commit completed",
                              message.sender, last_sent_index)
            return True
        # counting this node, so replies plus 1
        expected_confirms = (self.server.total_nodes - 1) / 2
        received_confirms = 0
        for follower in self.followers.values():
            if follower.last_index == last_sent_index:
                received_confirms += 1
            self.logger.debug("confirmation of log rec %d received from %s "\
                              "brings total to %d out of %d",
                              last_index, message.sender,
                              received_confirms, expected_confirms)
            
        if received_confirms < expected_confirms:
            self.logger.debug("not enough votes to commit yet")
            return True
        # we have enough to commit, so do it
        log.commit(last_sent_index)
        commit_rec = log.get(last_sent_index)
        if last_sent_index == 0:
            prev_index = None
            prev_term = None
        self.logger.debug("after commit, commit_index = %s",
                          log.get_commit_index())

        # now broadcast a commit append
        await self.send_append_entries(all, commit_for_rec=commit_rec)

        # now see if there is a client to send the reply to
        # for the completed record
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
                                           term,
                                           commit_rec.user_data)
        self.logger.debug("sending reply message %s", reply)
        await self.server.post_message(reply)
        return True

    async def send_append_entries(self, addr="all", start_index=None,
                                  end_index=None, commit_for_rec=None ):
        entries = []
        if commit_for_rec is None:
            if start_index is None:
                last_rec = log.read()
                start_index = last_rec.index
                entries.append(asdict(last_rec))
            else:
                if end_index not None:
                    for i in range(start_index, end_index+1):
                        rec = log.read(i)
                        entries.append(asdict(rec))
                else:
                    rec = log.read(start_index)
                    entries.append(asdict(rec))
            if start_index == 0:
                prev_index = None
                prev_term = None
            else:
                prev_rec = log.read(start_index -1)
                prev_index = prev_rec.index
                prev_term = prev_rec.term
        else:
            prev_index = commit_for_rec.index
            prev_term = commit_for_rec.term
            entries = []
        message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "prevLogIndex": prev_index,
                "prevLogTerm": prev_term,
                "entries": entries,
                "leaderCommit": log.get_commit_index(),
            }
        )
        if addr == "all":
            self.logger.debug("(term %d) sending log update to all" \
                              " followers: %s",
                              log.get_term(), update_message.data)
            await self.server.broadcast(update_message)
            if not commit_for_rec:
                await self.set_substate(Substate.sent_new_entries)
        else:
            message._receiver = addr
            self.logger.debug("(term %d) sending log update with" \
                              " %d entries to %s",
                              log.get_term(), len(entries), addr )
            await self.server.post_message(reply)
        
    async def do_backdown(self, target, rejected_index):
        log = self.server.get_log()
        if rejected_index == 0:
            raise Exception('cannot back down beyond index 0')
        new_index = rejected_index - 1
        await self.send_append_entries(target, new_index)
        
    async def on_log_pull(self, message):
        # follwer wants log messages that it has not received
        start_index = message.data['start_index']
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec is None:
            self.logger.info("follower %s asking for log pull but log empty", message.sender)
            reply = LogPullResponseMessage(
                self.server.endpoint,
                message.sender,
                log.get_term(),
                {
                    "code": "reset",
                    "leaderId": self.server.name,
                    "leaderPort": self.server.endpoint,
                    "prevLogIndex": None,
                    "prevLogTerm": None,
                    "entries": [],
                    "leaderCommit": log.get_commit_index(),
                }
            )
            await self.server.post_message(reply)
            return True
        if start_index is None:
            start_index = 0
        commit_limit = log.get_commit_index()
        if commit_limit is None:
            commit_limit = -1
        if start_index > last_rec.index or start_index > commit_limit:
            self.logger.info("follower %s asking for log pull from index %s " \
                             " beyond log limit %s or commit %s",
                             message.sender, start_index,
                             last_rec.index, log.get_commit_index())
            reply = LogPullResponseMessage(
                self.server.endpoint,
                message.sender,
                log.get_term(),
                {
                    "code": "reset",
                    "leaderId": self.server.name,
                    "leaderPort": self.server.endpoint,
                    "prevLogIndex": last_rec.index,
                    "prevLogTerm": last_rec.term,
                    "entries": [],
                    "leaderCommit": log.get_commit_index(),
                }
            )
            await self.server.post_message(reply)
            return True
        entries = []
        for i in range(start_index, last_rec.index +1):
            a_rec = log.read(i)
            entries.append(asdict(a_rec))
        reply = LogPullResponseMessage(
            self.server.endpoint,
            message.sender,
            log.get_term(),
            {
                "code": "apply",
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "prevLogIndex": last_rec.index,
                "prevLogTerm": last_rec.term,
                "entries": entries,
                "leaderCommit": log.get_commit_index(),
            }
        )
        await self.server.post_message(reply)
        return True
        
    async def send_heartbeat(self):
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        message = HeartbeatMessage(
            self.server.endpoint,
            None,
            log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "prevLogIndex": last_index,
                "prevLogTerm": last_term,
                "entries": [],
                "leaderCommit": log.get_commit_index(),
            }
        )
        self.heartbeat_logger.debug("sending heartbeat to all commit = %s",
                                    message.data['leaderCommit'])
        await self.server.broadcast(message)
        self.heartbeat_logger.debug("sent heartbeat to all commit = %s",
                                    message.data['leaderCommit'])
        await self.set_substate(Substate.sent_heartbeat)

    async def send_term_start(self):
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        data = {
            "leaderId": self.server.name,
            "leaderPort": self.server.endpoint,
            "prevLogIndex": last_index,
            "prevLogTerm": last_term,
            "leaderCommit": log.get_commit_index(),
        }
        message = TermStartMessage(
            self.server.endpoint,
            None,
            log.get_term(),
            data
        )
        # lets wait for messages to go out before noting change
        self.logger.info("sending term start message to all %s %s",
                         message, data)
        await self.server.broadcast(message)
        self.logger.info("sent term start message to all %s %s", message, data)
        await self.set_substate(Substate.sent_term_start)

    async def on_client_command(self, message):
        target = message.sender
        if message.original_sender:
            target = message.original_sender
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            new_index = last_rec.index + 1
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            new_index = 0
            # no log records yet
            last_index = None
            last_term = None

        # call the user app 
        result = self.server.get_app().execute_command(message.data)
        if not result.log_response:
            # user app does not want to log response
            self.logger.debug("preparing no-log reply for %s",
                              target)
            reply = ClientCommandResultMessage(self.server.endpoint,
                                               target,
                                               last_term,
                                               result.response)
            self.logger.debug("sending no-log reply message %s", reply)
            await self.server.post_message(reply)
            return True
        self.logger.debug("saving address for reply %s", target)

        # Before appending, get the index and term of the previous record,
        # this will tell the follower to check their log to make sure they
        # are up to date except for the new record(s)
        last_rec = log.read()
        new_rec = LogRec(term=log.get_term(), user_data=result.response,
                         context=dict(client_addr=target))
        log.append([new_rec,])
        new_rec = log.read()
        update_message = AppendEntriesMessage(
            self.server.endpoint,
            None,
            log.get_term(),
            {
                "leaderId": self.server.name,
                "leaderPort": self.server.endpoint,
                "prevLogIndex": last_index,
                "prevLogTerm": last_term,
                "entries": [asdict(new_rec),],
                "leaderCommit": log.get_commit_index(),
            }
        )
        self.logger.debug("(term %d) sending log update to all followers: %s",
                          log.get_term(), update_message.data)
        await self.server.broadcast(update_message)
        await self.set_substate(Substate.sent_new_entries)
        return True

    async def on_vote_received(self, message):
        log = self.server.get_log()
        self.logger.info("leader ignoring vote reply: message.term = %d local_term = %d",
                         message.term, log.get_term())
        return True

    async def on_vote_request(self, message): 
        log = self.server.get_log()
        self.logger.info("vote request from %s, sending am leader",
                         message.sender)
        reply = RequestVoteResponseMessage(self.server.endpoint,
                                         message.sender,
                                         log.get_term(),
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
        self.logger.warning("Why am I getting hearbeat when I am leader?")
        return True


