import random
import logging

from .voter import Voter
from .leader import Leader
from ..messages.request_vote import RequestVoteMessage
from .timer import Timer
logger = logging.getLogger(__name__)


# Raft Candidate. Transition state between Follower and Leader
class Candidate(Voter):

    def __init__(self, timeout=0.5):
        Voter.__init__(self)
        self._timeout = timeout

    def __str__(self):
        return "candidate"
    
    def set_server(self, server):
        self._server = server
        self._votes = {}
        self.candidate_timer = Timer(self.candidate_interval(), self._resign)
        self._start_election()

    def candidate_interval(self):
        return random.uniform(0, self._timeout)

    def on_append_entries(self, message):
        logger.info("candidate resigning because we got new entries")
        self._resign()

    def on_vote_received(self, message):
        # reset timer
        self.candidate_timer.reset()
        logger.info("vote received from %s, response %s", message.sender,
                    message.data['response'])
        if message.sender[1] not in self._votes and message.data['response']:
            self._votes[message.sender[1]] = message.data['response']

            # check if received majorities
            #if len(self._votes.keys()) > (self._server._total_nodes - 1) / 2:
            # my own vote is implied
            if len(self._votes.keys()) + 1 > self._server._total_nodes / 2:
                self.candidate_timer.stop()
                leader = Leader()
                self._server._state = leader
                leader.set_server(self._server)
                return leader, None

        # check if received all the votes -> resign
        if len(self._votes) == len(self._server.other_nodes):
            logger.info("candidate resigning because all votes are in but we didn't win")
            self._resign()
        else:
            return self, None

    # start elections by increasing term, voting for itself and send out vote requests
    def _start_election(self):
        self.candidate_timer.start()
        self._server._currentTerm += 1
        logger.info("candidate starting election term is %d", self._server._currentTerm)
        election = RequestVoteMessage(
            self._server.endpoint,
            None,
            self._server._currentTerm,
            {
                "lastLogIndex": self._server._lastLogIndex,
                "lastLogTerm": self._server._lastLogTerm,
            }
        )
        self._server.broadcast(election)
        self._last_vote = self._server.endpoint

    # received append entry from leader or not enough votes -> step down
    def _resign(self):
        self.candidate_timer.stop()

        logger.info("candidate resigning")
        from .follower import Follower
        follower = Follower()
        self._server._state = follower
        follower.set_server(self._server)
        return follower, None
