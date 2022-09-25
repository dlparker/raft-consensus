from .voter import Voter
from .timer import Timer
from .candidate import Candidate

import random
import asyncio
import logging

# Raft follower. Turns to candidate when it timeouts without receiving heartbeat from leader
class Follower(Voter):

    def __init__(self, timeout=0.75, vote_at_start=False):
        Voter.__init__(self)
        self._timeout = timeout
        self._leaderPort = None
        self._vote_at_start = vote_at_start
        # get this too soon and logging during testing does not work
        self.logger = logging.getLogger(__name__)
        
    def __str__(self):
        return "follower"
    
    def set_server(self, server):
        self._server = server
        interval = self.election_interval()
        self.election_timer = Timer(interval, self._start_election)
        self.election_timer.start()
        if self._vote_at_start:
            asyncio.get_event_loop().call_soon(self._start_election)

    def election_interval(self):
        return random.uniform(self._timeout, 2 * self._timeout)

    def _start_election(self):
        self.election_timer.stop()
        candidate = Candidate()
        self._server._state = candidate
        candidate.set_server(self._server)
        return candidate, None

    def on_append_entries(self, message):
        # reset timeout
        self.election_timer.reset()

        if message.term < self._server._currentTerm:
            self._send_response_message(message, votedYes=False)
            self.logger.debug("rejecting message because sender term is less than mine %s", message)
            return self, None

        if message.data != {}:
            log = self._server._log
            data = message.data
            self._leaderPort = data["leaderPort"]

            # Check if leader is too far ahead in log
            if data['leaderCommit'] != self._server._commitIndex:
                # if so then we use length of log - 1
                self._server._commitIndex = min(data['leaderCommit'], len(log) - 1)

            # If log is smaller than prevLogIndex -> not up-to-date
            if len(log)-1 < data["prevLogIndex"]:
                self._send_response_message(message, votedYes=False)
                self.logger.debug("rejecting message because our index is %s and sender is %s",
                             len(log)-1, data["prevLogIndex"])
                return self, None

            # make sure prevLogIndex term is always equal to the server
            if len(log) > 1 and log[data["prevLogIndex"]]["term"] != data["prevLogTerm"]:
                # conflict detected -> resync and delete everything from this
                # prevLogIndex and forward (extraneous entries)
                # send a failure to server
                print('Follower conflicting??')
                print(log)
                print(data)
                log = log[:data["prevLogIndex"]]
                self._send_response_message(message, votedYes=False)
                self._server._log = log
                self._server._lastLogIndex = data["prevLogIndex"]
                self._server._lastLogTerm = data["prevLogTerm"]
                return self, None
            else:
                # check if this is a heartbeat
                if len(data["entries"]) > 0:
                    self.logger.debug("accepting message on our index=%s and sender=%s, data=%s",
                                 len(log)-1, data["prevLogIndex"], data)
                    for entry in data["entries"]:
                        log.append(entry)
                        self._server._commitIndex += 1

                    self._server._lastLogIndex = len(log) - 1
                    self._server._lastLogTerm = log[-1]["term"]
                    self._commitIndex = len(log) - 1
                    self._server._log = log

                    self._send_response_message(message)

            self._send_response_message(message)
            return self, None
        else:
            return self, None

    def on_client_command(self, command, client_port):
        message = {
            'command': command,
            'client_port': client_port,
        }
        asyncio.ensure_future(self._server.post_message(message), loop=self._server._loop)
        return True
