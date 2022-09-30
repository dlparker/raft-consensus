import random
import traceback
import logging

from .voter import Voter
from .leader import Leader
from ..messages.request_vote import RequestVoteMessage
from .timer import Timer


# Raft Candidate. Transition state between Follower and Leader
class Candidate(Voter):

    _type = "candidate"
    
    def __init__(self, server, timeout=0.5):
        Voter.__init__(self)
        self._timeout = timeout
        self.logger = logging.getLogger(__name__)
        self._server = server
        server.set_state(self)
        self._votes = {}
        self.candidate_timer = self._server.get_timer("candidate-interval",
                                                     self.candidate_interval(),
                                                     self.on_timer)
        self._start_election()
                            
    def __str__(self):
        return "candidate"
    
    def candidate_interval(self):
        return random.uniform(0, self._timeout)

    def on_append_entries(self, message):
        self.logger.info("candidate resigning because we got new entries")
        self._resign()

    def on_heartbeat(self, message):
        self.logger.info("candidate resigning because we got hearbeat from leader")
        self._resign()
        self.on_heartbeat_common(message)
        self.logger.debug("sent heartbeat reply")

    def on_timer(self):
        self.logger.info("candidate resigning because timer ended")
        self._resign()

    def on_vote_received(self, message):
        # reset timer
        self.candidate_timer.reset()
        self.logger.info("vote received from %s, response %s", message.sender,
                    message.data['response'])
        if message.sender[1] not in self._votes and message.data['response']:
            self._votes[message.sender[1]] = message.data['response']

            # check if received majorities
            # if len(self._votes.keys()) > (self._server._total_nodes - 1) / 2:
            # The above original logic from upstream was wrong, it does
            # not work with three servers if the leader dies, election
            # cannot complete because number of votes receive cannot be more
            # than one. 
            # I changed the logic to include the fact that a candidate's
            # own vote is included in the total votes, which makes sense.
            # It is easy to see how you could read it the other way from
            # the text of the paper, but it does not work for and election
            # held by two out of three servers.
            # with one dead.
            if len(self._votes.keys()) + 1 > self._server._total_nodes / 2:
                self.candidate_timer.stop()
                leader = Leader(self._server)
                self.logger.info("changing to leader")
                return leader, None

        # check if received all the votes -> resign
        if len(self._votes) == len(self._server.other_nodes):
            self.logger.info("candidate resigning because all votes are in but we didn't win")
            self._resign()
        else:
            return self, None

    # start elections by increasing term, voting for itself and send out vote requests
    def _start_election(self):
        log = self._server.get_log()
        log_tail =  log.get_tail()
        self.candidate_timer.start()
        # CHANGE_TRACE: self._server._currentTerm += 1
        log.incr_term()

        self.logger.info("candidate starting election term is %d",
                         log.get_term())
        
        election = RequestVoteMessage(
            self._server.endpoint,
            None,
            log.get_term(),
            {
                "lastLogIndex": log_tail.last_index,
                "lastLogTerm": log_tail.term,
            }
        )
        self._server.broadcast(election)
        self._last_vote = self._server.endpoint

    # received append entry from leader or not enough votes -> step down
    def _resign(self):
        self.candidate_timer.stop()

        from .follower import Follower
        follower = Follower(server=self._server)
        self._server._state = follower
        follower.set_server(self._server)
        self.logger.info("candidate resigned")
        return follower, None

    def on_client_command(self, command, client_port):
        raise NotImplementedError

    def on_append_response(self, message):
        raise NotImplementedError
