from collections import defaultdict
import random
import asyncio
import logging
from dataclasses import dataclass, field

from .base_state import State
from .log_api import LogRec
from ..messages.append_entries import AppendEntriesMessage
from ..messages.command import ClientCommandResultMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from ..messages.termstart import TermStartMessage
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
        self._nextIndexes = defaultdict(int)
        self._matchIndex = defaultdict(int)
        self._heartbeat_timeout = heartbeat_timeout
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        self._server = server
        server.set_state(self)
        log = self._server.get_log()
        log_tail =  log.get_tail()
        self.logger.info('Leader on %s in term %s', self._server.endpoint,
                         log.get_term())
        self.followers = {}
        for other in self._server.other_nodes:
            # Assume follower is in sync, meaning we only send on new
            # log entry. If they are not, they will tell us that on
            # first heartbeat, and we will catch them up
            self.followers[other] = FollowerCursor(other, log_tail.last_index + 1)
        
        # Notify others of term start, just to make it official
        self.send_term_start()
        self.heartbeat_timer = self._server.get_timer("leader-heartbeat",
                                                      self._heartbeat_interval(),
                                                      self.send_heartbeat)
        self.heartbeat_timer.start()
        for other in self._server.other_nodes:
            self._nextIndexes[other[1]] = log_tail.last_index + 1
            self._matchIndex[other[1]] = 0

    def __str__(self):
        return "leader"
    
    def _heartbeat_interval(self):
        return random.uniform(0, self._heartbeat_timeout)

    def on_heartbeat_response(self, message):
        self.heartbeat_logger.debug("got heartbeat response from %s", message.sender)

    def on_heartbeat(self, message):
        self.logger.warning("Why am I getting hearbeat when I am leader?")
        #self.on_heartbeat_common(self, message)

    def on_log_pull(self, message):
        # Follower is asking for log records
        # for now we continue with the older walkback algo until
        # we get this new logic working, then we'll fix the alqo
        # so that the follower tells us were to start. This message
        # comes in after a heartbeat or append entry when the
        # follower examins the leader's log state, so the follower
        # knows how far behind it is
        log = self._server.get_log()
        log_tail =  log.get_tail()
        fc = self.followers[message.sender]
        prev_index = max(0, fc.next_index - 1)
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
                              message.sender, prev_index)
            return self, None

        # Tell the follower which log record should proceed the one we are sending
        # so the follower can check to see if they have it.
        last_props = log.get_index_context(current_rec.index - 1)
        append_entry = AppendEntriesMessage(
            self._server.endpoint,
            message.sender,
            log.get_term(),
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": last_props['index'],
                "prevLogTerm": last_props['term'],
                "entries": [current_rec.user_data,],
                "leaderCommit": log_tail.commit_index,
            })
        self.logger.debug("sending log entry prev at %s term %s to %s",
                          prev_index, prev_term, message.sender)
        asyncio.ensure_future(self._server.post_message(append_entry))
        
    def on_append_response_received(self, message):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        message_commit = message.data['leaderCommit']
        if log_tail.commit_index != message_commit:
            # this is a common occurance since we commit after a quorum
            self.logger.debug("got append response message from %s but " \
                                "all log messages already committed", message.sender)
            self.logger.debug("message %s log_tail %s", message.data, log_tail)
            return
        sender_index = message.data['prevLogIndex']
        if log_tail.last_index != sender_index + 1:
            self.logger.error("got append response message from %s for index %s but " \
                              "log index is up to %s, should be one less",
                              message.sender, sender_index, log_tail.last_index)
            return
            
        fc = self.followers[message.sender]
        # Append response means that follower log agrees with ours on
        # index, term, commit, etc. So their index is the same as
        # the one we sent, record it.
        fc.last_index = log_tail.last_index
        # They have all the records we have.
        # So, the next time they ask need a record will be the next time
        # we insert one.
        fc.next_index += 1

        # counting this node, so replies plus 1
        expected_confirms = (self._server._total_nodes - 1) / 2
        received_confirms = 0
        for follower in self.followers.values():
            if follower.last_index == log_tail.last_index:
                received_confirms += 1
        self.logger.debug("confirmation of log rec %d received from %s "\
                          "brings total to %d out of %d",
                          log_tail.last_index, message.sender,
                          received_confirms, expected_confirms)
                
        if received_confirms >= expected_confirms:
            self.logger.debug("commiting log rec %d", log_tail.last_index)
            log_tail = log.commit(log_tail.last_index)
            last_log = log.read(log_tail.commit_index)
            self.logger.debug("after commit, commit_index = %s", log_tail.commit_index)
            self.logger.debug("after commit, last_log = %s", last_log)
            self.logger.debug("after commit, last_log.data = %s", last_log.user_data)
            reply_address = last_log.user_data.get('reply_address', None)
            if reply_address:
                # This log record was for data submitted by client,
                # not an internal record such as term change.
                # make sure provided address is formated as a tuple
                # and use it to send reply to client
                reply_address = (reply_address[0], reply_address[1])
                response = last_log.user_data['response']
                self.logger.debug("preparing reply for %s from %s",
                                  response, reply_address)
                reply = ClientCommandResultMessage(self._server.endpoint,
                                                   reply_address,
                                                   log_tail.term,
                                                   response)
                self.logger.debug("sending reply message %s", reply)
                asyncio.ensure_future(self._server.post_message(reply))
        
    def on_append_response(self, message):
        # check if last append_entries good?
        log = self._server.get_log()
        log_tail =  log.get_tail()
        if not message.data["response"]:
            return self.on_log_pull(message)
        else:
            self.on_append_response_received(message)

        return self, None
    
    def send_heartbeat(self):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        last_props = log.get_index_context()
        message = HeartbeatMessage(
            self._server.endpoint,
            None,
            log.get_term(),
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": last_props['index'],
                "prevLogTerm": last_props['term'],
                "entries": [],
                "leaderCommit": log_tail.commit_index,
            }
        )
        self._server.broadcast(message)
        self.heartbeat_logger.debug("sent heartbeat to all")

    def send_term_start(self):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        last_props = log.get_index_context()
        data = {
            "leaderId": self._server._name,
            "leaderPort": self._server.endpoint,
            "prevLogIndex": last_props['index'],
            "prevLogTerm": last_props['term'],
            "entries": [],
            "leaderCommit": log_tail.commit_index,
        }
        message = TermStartMessage(
            self._server.endpoint,
            None,
            log.get_term(),
            data
        )
        self._server.broadcast(message)
        self.logger.info("sent term start message to all %s %s", message, data)

    def on_client_command(self, message, client_port):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        target = client_port
        if message.original_sender:
            target = message.original_sender
        self.logger.debug("saving address for reply %s",
                          target)
        response, balance = self.execute_command(message)
        if response == "Invalid command":
            return False
        entries = [{
            "term": log.get_term(),
            "log_index": log_tail.last_index + 1,
            "command": message.data,
            "balance": balance,
            "response": response,
            "reply_address": target
        }]
        # Before appending, get the index and term of the previous record,
        # this will tell the follower to check their log to make sure they
        # are up to date except for the new record(s)
        last_props = log.get_index_context()
        log.append([LogRec(term=log.get_term(), user_data=entries[0]),])
        update_message = AppendEntriesMessage(
            self._server.endpoint,
            None,
            log.get_term(),
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": last_props['index'],
                "prevLogTerm": last_props['term'],
                "entries": entries,
                "leaderCommit": log_tail.commit_index,
            }
        )
        self.logger.debug("(term %d) sending log update to all followers: %s",
                          log.get_term(), update_message.data)
        self._server.broadcast(update_message)
        return True

    def execute_command(self, message):
        command = message.data.split()
        log = self._server.get_log()
        log_tail = log.get_tail()
        log_rec = log.read(log_tail.last_index)
        balance = log_rec.user_data['balance'] or 0
        if len(command) == 0:
            response = "Invalid command"
        elif len(command) == 1 and command[0] == 'query':
            response = "Your current account balance is: " + str(balance)
        elif len(command) == 2 and command[0] == 'credit':
            if int(command[1]) <= 0:
                response = "Credit amount must be positive"
            else:
                response = "Successfully credited " + command[1] + " to your account"
                balance += int(command[1])
        elif len(command) == 2 and command[0] == 'debit':
            if balance >= int(command[1]):
                if int(command[1]) <= 0:
                    response = "Debit amount must be positive"
                else:
                    response = "Successfully debited " + command[1] + " from your account"
                    balance -= int(command[1])
            else:
                response = "Insufficient account balance"
        else:
            response = "Invalid command"
        if response == "Invalid command":
            self.logger.debug("invalid client command %s", command)
        else:
            self.logger.debug("completed client command %s", command)
        return response, balance

    def on_vote_received(self, message):
        log = self._server.get_log()
        self.logger.info("leader got vote: message.term = %d local_term = %d",
                         message.term, log.get_term())

    def on_vote_request(self, message):
        self.logger.warning("got unexpected vote request from %s", message.sender)
    
    def on_append_entries(self, message):
        self.logger.warning("got unexpected vote request from %s", message.sender)
    
    def on_term_start(self, message):
        self.logger.warning("leader got term start message from %s, makes no sense!",
                         message.sender) 
