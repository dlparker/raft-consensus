from collections import defaultdict
import random
import asyncio
import logging
from .base_state import State
from .log_api import LogRec
from ..messages.append_entries import AppendEntriesMessage
from .timer import Timer
logger = logging.getLogger(__name__)


# Raft leader. Currently does not support step down -> leader will stay forever until terminated
class Leader(State):

    def __init__(self, heartbeat_timeout=0.5):
        self._nextIndexes = defaultdict(int)
        self._matchIndex = defaultdict(int)
        self._heartbeat_timeout = heartbeat_timeout

    def __str__(self):
        return "leader"
    
    def set_server(self, server):
        self._server = server
        # send heartbeat immediately
        logger.info('Leader on %s in term %s', self._server.endpoint,
                    self._server._currentTerm)
        self._send_heartbeat()
        self.heartbeat_timer = Timer(self._heartbeat_interval(), self._send_heartbeat)
        self.heartbeat_timer.start()

        for other in self._server.other_nodes:
            self._nextIndexes[other[1]] = self._server._lastLogIndex + 1
            self._matchIndex[other[1]] = 0

    def _heartbeat_interval(self):
        return random.uniform(0, self._heartbeat_timeout)

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
                logger.error("cannot find log message %d for other node %s",
                             prevIndex, message.sender)
                return self, None
            prev = prev_rec.user_data
            next_i = self._nextIndexes[message.sender[1]]
            current_rec = log.read(next_i)
            if not current_rec:
                logger.error("cannot find log message %d for other node %s",
                             next_i, message.sender)
                return self, None
            
            # send new log to client and wait for respond
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
            asyncio.ensure_future(self._server.post_message(append_entry))
        else:
            # last append was good -> increase index
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
                if matchIndex == (self._server._lastLogIndex):
                    majority_response_received += 1

            if (majority_response_received >= (self._server._total_nodes - 1) / 2
                and log_tail.last_index > 0 and log_tail.last_index == log_tail.commit_index +1):

                client_addr = 'localhost', self._server.client_port
                # committing next index

                log.commit(log_tail.commit_index + 1)
                last_log = log.read(log_tail.last_index)
                response = last_log.user_data['response']
                message = {
                    'receiver': client_addr,
                    'value': response
                }
                logger.debug("sending reply message %s", message)
                asyncio.ensure_future(self._server.post_message(message))

        return self, None

    def _send_heartbeat(self):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        message = AppendEntriesMessage(
            self._server.endpoint,
            None,
            self._server._currentTerm,
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": self._server._lastLogIndex,
                "prevLogTerm": self._server._lastLogTerm,
                "entries": [],
                "leaderCommit": log_tail.commit_index,
            }
        )
        self._server.broadcast(message)

    def on_client_command(self, message, client_port):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        self._server.client_port = client_port
        response, balance = self.execute_command(message)
        if response == "Invalid command":
            return False
        entries = [{
            "term": log_tail.term,
            "command": message,
            "balance": balance,
            "response": response
        }]
        log = self._server.get_log()
        pre_save_log_tail = log.get_tail()
        log.append([LogRec(user_data=entries[0]),], self._server._currentTerm)

        message = AppendEntriesMessage(
            self._server.endpoint,
            None,
            self._server._currentTerm,
            {
                "leaderId": self._server._name,
                "leaderPort": self._server.endpoint,
                "prevLogIndex": pre_save_log_tail.last_index,
                "prevLogTerm": pre_save_log_tail.last_term,
                "entries": entries,
                "leaderCommit": pre_save_log_tail.commit_index,
            }
        )
        self._server.broadcast(message)
        return True

    def execute_command(self, command):
        command = command.split()
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
        return response, balance

    def on_vote_received(self, message):
        logger.info("leader got vote: message.term = %d local_term = %d",
                    message.term, self._server._currentTerm)

    def on_vote_request(self, message):
        raise NotImplementedError
    
    def on_append_entries(self, message):
        raise NotImplementedError
    
