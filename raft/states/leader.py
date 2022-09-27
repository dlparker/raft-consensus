from collections import defaultdict
import random
import asyncio
import logging
from .base_state import State
from .log_api import LogRec
from ..messages.append_entries import AppendEntriesMessage
from ..messages.command import ClientCommandResultMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from .timer import Timer


# Raft leader. Currently does not support step down -> leader will stay forever until terminated
class Leader(State):

    _type = "leader"
    
    def __init__(self, heartbeat_timeout=0.5):
        self._nextIndexes = defaultdict(int)
        self._matchIndex = defaultdict(int)
        self._heartbeat_timeout = heartbeat_timeout
        self.logger = logging.getLogger(__name__)

    def __str__(self):
        return "leader"
    
    def set_server(self, server):
        self._server = server
        # send heartbeat immediately
        self.logger.info('Leader on %s in term %s', self._server.endpoint,
                    self._server._currentTerm)
        self._send_heartbeat()
        self.heartbeat_timer = Timer(self._heartbeat_interval(), self._send_heartbeat)
        self.heartbeat_timer.start()

        log = self._server.get_log()
        log_tail =  log.get_tail()
        for other in self._server.other_nodes:
            self._nextIndexes[other[1]] = log_tail.last_index + 1
            self._matchIndex[other[1]] = 0

    def _heartbeat_interval(self):
        return random.uniform(0, self._heartbeat_timeout)

    def on_heartbeat_response(self, message):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        self.logger.debug("heartbeat response from %s with "\
                          "log tail = %s msg_data= %s",
                          message.sender,
                          log_tail,
                          message.data)

    def on_heartbeat(self, message):
        self.logger.warning("Why am I getting hearbeat when I am leader?")
        #self.on_heartbeat_common(self, message)
        
    def on_response_received(self, message):
        # check if last append_entries good?
        log = self._server.get_log()
        log_tail =  log.get_tail()
        if not message.data["response"]:
            # Other node has an out of date log. Back up
            # one log record and send that one. This may
            # continue for multiple passes until the other
            # node says it is happy with the new record. 
            # if not, back up log for this node. Once it
            # does, we will keep sending until caught up.
            self._nextIndexes[message.sender[1]] -= 1
            # get next log entry to send to follower
            prevIndex = max(0, self._nextIndexes[message.sender[1]] - 1)
            prev_rec = log.read(prevIndex)
            if not prev_rec:
                self.logger.error("cannot find log message %d for other node %s",
                             prevIndex, message.sender)
                return self, None
            prev = prev_rec.user_data
            next_i = self._nextIndexes[message.sender[1]]
            current_rec = log.read(next_i)
            if not current_rec:
                self.logger.error("cannot find log message %d for other node %s",
                             next_i, message.sender)
                return self, None
            
            # send new log to other and wait for respond
            append_entry = AppendEntriesMessage(
                self._server.endpoint,
                message.sender,
                self._server._currentTerm,
                {
                    "leaderId": self._server._name,
                    "leaderPort": self._server.endpoint,
                    "prevLogIndex": prevIndex,
                    "prevLogTerm": prev["term"],
                    "entries": [current_rec.user_data,],
                    "leaderCommit": log_tail.commit_index,
                })
            self.logger.debug("sending log entry prev at %s term %s to %s",
                              prevIndex, prev['term'], message.sender)
            asyncio.ensure_future(self._server.post_message(append_entry))
        else:
            # last append was good -> increase index
            # TODO: fix the index logic for these bookkeeping lists,
            # current logic is port number, which could be the same
            # on different machines. Use the whole address tuple instead
            self._nextIndexes[message.sender[1]] += 1
            self._matchIndex[message.sender[1]] += 1

            # check if caught up?
            # this logic seems a bit strange, can next index for send be more
            # than one higher than the last local log?
            # TODO: see if we can clear this up
            if self._nextIndexes[message.sender[1]] > log_tail.last_index:
                self._nextIndexes[message.sender[1]] = log_tail.last_index + 1
                self._matchIndex[message.sender[1]] = log_tail.last_index

            majority_response_received = 0

            for follower, matchIndex in self._matchIndex.items():
                if matchIndex == log_tail.last_index:
                    majority_response_received += 1
            self.logger.debug("processing response from %s with "\
                              "next_i %d, tail = %s tally is now %d, msg_data= %s",
                              message.sender,
                              self._nextIndexes[message.sender[1]],
                              log_tail,
                              majority_response_received,
                              message.data)

            if (majority_response_received >= (self._server._total_nodes - 1) / 2
                and log_tail.last_index > 0
                and log_tail.last_index == log_tail.commit_index +1):

                # committing next index

                log.commit(log_tail.commit_index + 1)
                last_log = log.read(log_tail.last_index)
                response = last_log.user_data['response']
                reply_address = last_log.user_data['reply_address']
                # make sure it is a tuple
                reply_address = (reply_address[0], reply_address[1])
                self.logger.debug("preparing reply for %s from %s",
                                  response, reply_address)
                reply = ClientCommandResultMessage(self._server.endpoint,
                                                   reply_address,
                                                   log_tail.term,
                                                   response)
                self.logger.debug("sending reply message %s", reply)
                asyncio.ensure_future(self._server.post_message(reply))

        return self, None

    def _send_heartbeat(self):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        #message = HeartbeatMessage(
        message = AppendEntriesMessage(
            self._server.endpoint,
            None,
            self._server._currentTerm,
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": log_tail.last_index,
                "prevLogTerm": log_tail.term,
                "entries": [],
                "leaderCommit": log_tail.commit_index,
            }
        )
        self._server.broadcast(message)

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
            "term": self._server._currentTerm,
            "command": message.data,
            "balance": balance,
            "response": response,
            "reply_address": target
        }]
        log = self._server.get_log()
        log.append([LogRec(user_data=entries[0]),], self._server._currentTerm)

        update_message = AppendEntriesMessage(
            self._server.endpoint,
            None,
            self._server._currentTerm,
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": log_tail.last_index,
                "prevLogTerm": log_tail.term,
                "entries": entries,
                "leaderCommit": log_tail.commit_index,
            }
        )
        self.logger.debug("(term %d) sending log update to all followers: %s",
                          self._server._currentTerm, update_message.data)
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
        self.logger.info("leader got vote: message.term = %d local_term = %d",
                    message.term, self._server._currentTerm)

    def on_vote_request(self, message):
        raise NotImplementedError
    
    def on_append_entries(self, message):
        raise NotImplementedError
    
