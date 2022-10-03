from collections import defaultdict
import random
import asyncio
import logging
from dataclasses import dataclass, field, asdict

from .base_state import State
from ..log.log_api import LogRec
from ..messages.append_entries import AppendEntriesMessage
from ..messages.command import ClientCommandResultMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from ..messages.termstart import TermStartMessage
from ..messages.log_pull import LogPullResponseMessage
from .timer import Timer

@dataclass
class FollowerCursor:
    addr: str
    next_index: int
    last_index: int = field(default=0)

# Raft leader. Currently does not support step down -> leader will stay forever until terminated
class Leader(State):

    _type = "leader"
    
    def __init__(self, server, heartbeat_timeout=0.5):
        self._heartbeat_timeout = heartbeat_timeout
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        self._server = server
        server.set_state(self)
        log = self._server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index + 1
        else:
            # no log records yet
            last_index = -1
        self.logger.info('Leader on %s in term %s', self._server.endpoint,
                         log.get_term())
        self.followers = {}
        for other in self._server.other_nodes:
            # Assume follower is in sync, meaning we only send on new
            # log entry. If they are not, they will tell us that on
            # first heartbeat, and we will catch them up
            self.followers[other] = FollowerCursor(other, last_index)
        
        # Notify others of term start, just to make it official
        self.send_term_start()
        self.heartbeat_timer = self._server.get_timer("leader-heartbeat",
                                                      self._heartbeat_timeout,
                                                      self.send_heartbeat)
        self.heartbeat_timer.start()

    def __str__(self):
        return "leader"

    def get_leader_addr(self):
        return self._server.endpoint
    
    def on_heartbeat_response(self, message):
        self.heartbeat_logger.debug("got heartbeat response from %s",
                                    message.sender)

    def on_log_pull(self, message):
        # Follower is asking for log records
        # for now we continue with the older walkback algo until
        # we get this new logic working, then we'll fix the alqo
        # so that the follower tells us were to start. This message
        # comes in after a heartbeat or append entry when the
        # follower examins the leader's log state, so the follower
        # knows how far behind it is
        log = self._server.get_log()
        last_rec = log.read()
        if last_rec is None:
            # special case, log is empty
            self.logger.error("follower %s thinks there log records to pull but log is empty",
                              message.sender, prev_index)
            return self, None
        fc = self.followers[message.sender]
        if fc.next_index == 0:
            # special case, first log record
            prev_term = None
            prev_index = None
        else:
            prev_index = fc.next_index - 1
            prev_rec = log.read(prev_index)
            if not prev_rec:
                self.logger.error("cannot find last log message %d for follower %s",
                                  prev_index, message.sender)
                return self, None
            # this helps the follower validate the log position
            prev_term = prev_rec.user_data['term']
        # get the next record that they don't have
        current_rec = log.read(fc.next_index)
        if not current_rec:
            self.logger.error("follower %s thinks there are more log records "\
                              "after %d, but we don't",
                              message.sender, fc.next_index - 1)
            return self, None

        # Tell the follower which log record should proceed the one we are sending
        # so the follower can check to see if they have it.
        append_entry = AppendEntriesMessage(
            self._server.endpoint,
            message.sender,
            log.get_term(),
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": prev_index,
                "prevLogTerm": prev_term,
                "entries": [asdict(current_rec),],
                "leaderCommit": log.get_commit_index(),
            })
        self.logger.debug("sending log entry prev at %s term %s to %s",
                          prev_index, prev_term, message.sender)
        asyncio.ensure_future(self._server.post_message(append_entry))
        
    def on_append_response_received(self, message):
        log = self._server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        message_commit = message.data['leaderCommit']
        if log.get_commit_index() != message_commit:
            # this is a common occurance since we commit after a quorum
            self.logger.debug("got append response message from %s but " \
                                "all log messages already committed", message.sender)
            self.logger.debug("message from %s %s last_rec %s", message.sender,
                              message.data, last_rec)
            return
        sender_index = message.data['prevLogIndex']
        if sender_index and last_index and last_index != sender_index + 1:
            self.logger.error("got append response message from %s for index %s but " \
                              "log index is up to %s, should be one less",
                              message.sender, sender_index, last_index)
            return
            
        fc = self.followers[message.sender]
        # Append response means that follower log agrees with ours on
        # index, term, commit, etc. So their index is the same as
        # the one we sent, record it.
        fc.last_index = last_index
        # They have all the records we have.
        # So, the next time they ask need a record will be the next time
        # we insert one.
        fc.next_index += 1

        # counting this node, so replies plus 1
        expected_confirms = (self._server._total_nodes - 1) / 2
        received_confirms = 0
        for follower in self.followers.values():
            if follower.last_index == last_index:
                received_confirms += 1
        self.logger.debug("confirmation of log rec %d received from %s "\
                          "brings total to %d out of %d",
                          last_index, message.sender,
                          received_confirms, expected_confirms)
                
        if received_confirms >= expected_confirms:
            self.logger.debug("commiting log rec %d", last_index)
            log.commit(last_index)
            # get a new copy of the record, committed flag should be True now
            last_rec = log.read(last_index)
            self.logger.debug("after commit, commit_index = %s", log.get_commit_index())
            # now tell followers to commit too
            # get the prevIndex and prevTerm from the message, as that is what
            # we are commiting
            old_index = message.data['prevLogIndex']
            old_term = message.data['prevLogTerm']
            commit_message = AppendEntriesMessage(
                self._server.endpoint,
                None,
                log.get_term(),
                {
                    "leaderId": self._server._name,
                    "leaderPort": self._server.endpoint,
                    "prevLogIndex": old_index,
                    "prevLogTerm": old_term,
                    "entries": [],
                    "leaderCommit": log.get_commit_index(),
                }
            )
            self.logger.debug("(term %d) sending log commit to all followers: %s",
                              log.get_term(), commit_message.data)
            self._server.broadcast(commit_message)
            if last_rec.context is not None:
                reply_address = last_rec.context.get('client_addr', None)
                if reply_address:
                    # This log record was for data submitted by client,
                    # not an internal record such as term change.
                    # make sure provided address is formated as a tuple
                    # and use it to send reply to client
                    reply_address = (reply_address[0], reply_address[1])
                    self.logger.debug("preparing reply for %s",
                                      reply_address)
                    reply = ClientCommandResultMessage(self._server.endpoint,
                                                       reply_address,
                                                       last_term,
                                                       last_rec.user_data)
                    self.logger.debug("sending reply message %s", reply)
                    asyncio.ensure_future(self._server.post_message(reply))

    def on_log_pull(self, message):
        # follwer wants log messages that it has not received
        start_index = message.data['start_index']
        log = self._server.get_log()
        last_rec = log.read()
        if last_rec is None:
            self.logger.info("follower %s asking for log pull but log empty", message.sender)
            reply = LogPullResponseMessage(
                self._server.endpoint,
                message.sender,
                log.get_term(),
                {
                    "code": "reset",
                    "leaderId": self._server._name,
                    "leaderPort": self._server.endpoint,
                    "prevLogIndex": None,
                    "prevLogTerm": None,
                    "entries": [],
                    "leaderCommit": log.get_commit_index(),
                }
            )
            asyncio.ensure_future(self._server.post_message(reply))
            return self, None
        if start_index is None:
            start_index = 0
        if start_index > last_rec.index or start_index > log.get_commit_index():
            self.logger.info("follower %s asking for log pull %d beyond log limit %d or commit %d",
                             message.sender, start_index, last_rec.index, log.get_commit_index())
            reply = LogPullResponseMessage(
                self._server.endpoint,
                message.sender,
                log.get_term(),
                {
                    "code": "reset",
                    "leaderId": self._server._name,
                    "leaderPort": self._server.endpoint,
                    "prevLogIndex": last_rec.index,
                    "prevLogTerm": last_rec.term,
                    "entries": [],
                    "leaderCommit": log.get_commit_index(),
                }
            )
            asyncio.ensure_future(self._server.post_message(reply))
            return self, None
        entries = []
        for i in range(start_index, log.get_commit_index() +1):
            a_rec = log.read(i)
            entries.append(asdict(a_rec))
        reply = LogPullResponseMessage(
            self._server.endpoint,
            message.sender,
            log.get_term(),
            {
                "code": "apply",
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": last_rec.index,
                "prevLogTerm": last_rec.term,
                "entries": entries,
                "leaderCommit": log.get_commit_index(),
            }
        )
        asyncio.ensure_future(self._server.post_message(reply))
        return self, None
        
        
    def on_append_response(self, message):
        # check if last append_entries good?
        log = self._server.get_log()
        last_rec = log.read()
        if not message.data["response"]:
            return self.on_log_pull(message)
        else:
            self.on_append_response_received(message)

        return self, None
    
    def send_heartbeat(self):
        log = self._server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        message = HeartbeatMessage(
            self._server.endpoint,
            None,
            log.get_term(),
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": last_index,
                "prevLogTerm": last_term,
                "entries": [],
                "leaderCommit": log.get_commit_index(),
            }
        )
        self._server.broadcast(message)
        self.heartbeat_logger.debug("sent heartbeat to all")

    def send_term_start(self):
        log = self._server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        data = {
            "leaderId": self._server._name,
            "leaderPort": self._server.endpoint,
            "prevLogIndex": last_index,
            "prevLogTerm": last_term,
            "leaderCommit": log.get_commit_index(),
        }
        message = TermStartMessage(
            self._server.endpoint,
            None,
            log.get_term(),
            data
        )
        self._server.broadcast(message)
        self.logger.info("sent term start message to all %s %s", message, data)

    def on_client_command(self, message):
        target = message.sender
        if message.original_sender:
            target = message.original_sender
        self.logger.debug("saving address for reply %s",
                          target)
        data = self._server.get_app().execute_command(message.data)
        if data is None:
            return False
        log = self._server.get_log()
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

        # Before appending, get the index and term of the previous record,
        # this will tell the follower to check their log to make sure they
        # are up to date except for the new record(s)
        last_rec = log.read()
        new_rec = LogRec(term=log.get_term(), user_data=data,
                         context=dict(client_addr=target))
        log.append([new_rec,])
        new_rec = log.read()
        update_message = AppendEntriesMessage(
            self._server.endpoint,
            None,
            log.get_term(),
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": last_index,
                "prevLogTerm": last_term,
                "entries": [asdict(new_rec),],
                "leaderCommit": log.get_commit_index(),
            }
        )
        self.logger.debug("(term %d) sending log update to all followers: %s",
                          log.get_term(), update_message.data)
        self._server.broadcast(update_message)
        return True

    def on_vote_received(self, message): # pragma: no cover error
        log = self._server.get_log()
        self.logger.info("leader unexpectedly got vote: message.term = %d local_term = %d",
                         message.term, log.get_term())

    def on_vote_request(self, message): # pragma: no cover error
        self.logger.warning("got unexpectedly vote request from %s", message.sender)
    
    def on_append_entries(self, message): # pragma: no cover error
        self.logger.warning("got unexpectedly vote request from %s", message.sender)
    
    def on_term_start(self, message): # pragma: no cover error
        self.logger.warning("leader got term start message from %s, makes no sense!",
                         message.sender) 

    def on_heartbeat(self, message): # pragma: no cover error
        self.logger.warning("Why am I getting hearbeat when I am leader?")
        #self.on_heartbeat_common(self, message)

