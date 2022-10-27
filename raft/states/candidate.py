import random
import traceback
import logging
import asyncio

from ..messages.request_vote import RequestVoteMessage
from ..utils import task_logger
from .voter import Voter
from .leader import Leader
from .base_state import Substate


class Candidate(Voter):

    my_type = "candidate"
    
    def __init__(self, server, timeout=0.5):
        super().__init__(server, self.my_type)
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self.server = server
        server.set_state(self)
        self.yea_votes = {}
        self.all_votes = {}
        self.election_timeout = self.candidate_interval()
        self.candidate_timer = None
        self.task = None
                            
    def __str__(self):
        return "candidate"
    
    def start(self):
        if self.terminated:
            raise Exception("cannot start a terminated state")

        log = self.server.get_log()
        self.candidate_timer = self.server.get_timer("candidate-interval",
                                                     log.get_term(),
                                                     self.election_timeout,
                                                     self.on_timer)
        self.candidate_timer.start()
        self.task = task_logger.create_task(self.start_election(),
                                            logger=self.logger,
                                            message="candidate election error")
    async def stop(self):
        # ignore already terminated, just make sure it is done
        self.terminated = True
        if not self.candidate_timer.terminated:
            await self.candidate_timer.terminate()
        if self.task:
            self.task.cancel()
            await asyncio.sleep(0)
            
    def candidate_interval(self):
        return random.uniform(0.01, self.timeout)
        
    async def on_term_start(self, message):
        self.logger.info("candidate resigning because we got a term start message")
        await self.resign()
        return False
    
    async def on_append_entries(self, message):
        self.logger.info("candidate resigning because we got new entries")
        await self.resign()
        return True

    async def on_heartbeat(self, message):
        self.logger.info("candidate resigning because we" \
                         "got hearbeat from leader")
        await self.resign()
        self.logger.debug("rejecting heartbeat")
        return False

    async def on_timer(self):
        self.logger.info("candidate resigning because timer ended")
        await self.resign()
        return True

    async def on_vote_received(self, message):
        # reset timer
        #await self.candidate_timer.reset()
        self.logger.info("vote received from %s, response %s", message.sender,
                    message.data['response'])
        if message.data.get('already_leader', None):
            self.logger.info("candidate resigning because" \
                             " leader %s answered vote",
                             message.sender)
            await self.resign()
            return True
        if message.sender in self.all_votes:
            # already counted, dummy trying to cheat by voting twice
            return True
        self.all_votes[message.sender] = message.data['response']
        if not message.data['response']:
            if len(self.all_votes) == len(self.server.other_nodes):
                self.logger.info("candidate resigning because all " \
                                 "votes are in but we didn't win")
                await self.resign()
            return True
        self.yea_votes[message.sender] = True
        # check if received majorities
        # if len(self.yea_votes.keys()) > (self.server.total_nodes - 1) / 2:
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
        if len(self.yea_votes.keys()) + 1 > self.server.total_nodes / 2:
            sm = self.server.get_state_map()
            try:
                sm.start_state_change("candidate", "leader")
                self.terminated = True
                await self.candidate_timer.terminate() # never run again
                self.logger.info("changing to leader")
                leader = await sm.switch_to_leader(self)
                await self.stop()
            except:
                sm.failed_state_change("candidate", "leader",
                                       traceback.format_exc())
        return True

    # start elections by increasing term, voting for itself and send out vote requests
    async def start_election(self):
        if self.terminated:
            return
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        await self.candidate_timer.reset()
        log.incr_term()
        self.term = log.get_term()
        self.logger.info("candidate starting election term is %d, timeout is %f",
                         self.term, self.election_timeout)
        
        election = RequestVoteMessage(
            self.server.endpoint,
            None,
            log.get_term(),
            {
                "lastLogIndex": last_index,
                "lastLogTerm": last_term,
            }
        )
        await self.server.broadcast(election)
        await self.set_substate(Substate.voting)
        self.logger.info("sent all endpoints %s", election)
        self.last_vote = self.server.endpoint
        self.task = None

    # Voting ended by some combination of messages
    async def resign(self):
        if self.terminated:
            # order in async makes race for server states
            # switch and new timer fire
            return
        try:
            sm = self.server.get_state_map()
            sm.start_state_change("candidate", "follower")
            self.terminated = True
            await self.candidate_timer.terminate() # never run again
            follower = await sm.switch_to_follower(self)
            self.logger.info("candidate resigned")
            await self.stop()
        except:
            sm.failed_state_change("candidate", "follower",
                                   traceback.format_exc())

    def get_leader_addr(self):
        return None
    
    async def on_vote_request(self, message):
        self.logger.info("ignoring vote request from %s", message.sender)
        return True
        
    async def on_client_command(self, message):
        await self.dispose_client_command(message, self.server)
        return True

    async def on_append_response(self, message): # pragma: no cover error
        self.logger.warning("candidate unexpectedly got append response from %s",
                            message.sender)
        return True

    async def on_heartbeat_response(self, message):  # pragma: no cover error
        self.logger.warning("candidate unexpectedly got heartbeat response from %s",
                            message.sender)
        return True
