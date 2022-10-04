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
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self.server = server
        server.set_state(self)
        self.votes = {}
        self.candidate_timer = self.server.get_timer("candidate-interval",
                                                     self.candidate_interval(),
                                                     self.on_timer)
        self.start_election()
                            
    def __str__(self):
        return "candidate"
    
    def candidate_interval(self):
        return random.uniform(0, self.timeout)

    def on_term_start(self, message):
        self.logger.info("candidate resigning because we got a term start message")
        self.resign()
        
    def on_append_entries(self, message):
        self.logger.info("candidate resigning because we got new entries")
        self.resign()

    def on_heartbeat(self, message):
        self.logger.info("candidate resigning because we got hearbeat from leader")
        self.resign()
        self.on_heartbeat_common(message)
        self.logger.debug("sent heartbeat reply")

    def on_timer(self):
        self.logger.info("candidate resigning because timer ended")
        self.resign()

    def on_vote_received(self, message):
        # reset timer
        self.candidate_timer.reset()
        self.logger.info("vote received from %s, response %s", message.sender,
                    message.data['response'])
        if message.sender[1] not in self.votes and message.data['response']:
            self.votes[message.sender[1]] = message.data['response']

            # check if received majorities
            # if len(self.votes.keys()) > (self.server.total_nodes - 1) / 2:
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
            if len(self.votes.keys()) + 1 > self.server.total_nodes / 2:
                self.candidate_timer.stop()
                sm = self.server.get_state_map()
                leader = sm.switch_to_leader(self)
                #leader = Leader(self.server)
                self.logger.info("changing to leader")
                return leader, None

        # check if received all the votes -> resign
        if len(self.votes) == len(self.server.other_nodes):
            self.logger.info("candidate resigning because all votes are in but we didn't win")
            self.resign()
        else:
            return self, None

    # start elections by increasing term, voting for itself and send out vote requests
    def start_election(self):
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        self.candidate_timer.start()
        log.incr_term()

        self.logger.info("candidate starting election term is %d",
                         log.get_term())
        
        election = RequestVoteMessage(
            self.server.endpoint,
            None,
            log.get_term(),
            {
                "lastLogIndex": last_index,
                "lastLogTerm": last_term,
            }
        )
        self.server.broadcast(election)
        self.last_vote = self.server.endpoint

    # received append entry from leader or not enough votes -> step down
    def resign(self):
        self.candidate_timer.stop()
        sm = self.server.get_state_map()
        follower = sm.switch_to_follower(self)
        #from .follower import Follower
        #follower = Follower(server=self.server)
        #self.server.state = follower
        #follower.set_server(self.server)
        self.logger.info("candidate resigned")
        return follower, None

    def get_leader_addr(self):
        return None
    
    def on_client_command(self, message):
        self.dispose_client_command(message, self.server)

    def on_append_response(self, message): # pragma: no cover error
        self.logger.warning("candidate unexpectedly got append response from %s",
                            message.sender)

