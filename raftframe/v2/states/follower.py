import time
import logging
from raftframe.v2.states.base_state import StateCode, Substate, BaseState
from raftframe.v2.states.context import RaftContext
from raftframe.messages.append_entries import AppendResponseMessage
from raftframe.messages.request_vote import RequestVoteResponseMessage

class Follower(BaseState):

    def __init__(self, hull):
        super().__init__(hull, StateCode.follower)
        # log is set in BaseState
        self.term = self.log.get_term()
        self.commit_index = self.log.get_commit_index()
        # only known after first accepted append_entries call
        self.leader_uri = None
        # only used during voting for leadership
        self.last_vote = None
        # Needs to be as recent as configured maximum silence period, or we raise hell.
        # Pretend we just got a call, that gives possible actual leader time to ping us
        self.last_leader_contact = time.time()

    async def start(self):
        await super().start()
        self.last_leader_contact = time.time()
        await self.run_after(self.hull.get_leader_lost_timeout(), self.contact_checker)
        
    async def on_append_entries(self, message):
        self.logger.info("append term = %d prev_index = %d local_term = %d local_index = %d",
                         message.term, message.prevLogIndex, self.term, self.commit_index)

        # Read the following three if statements carefully before modifying.
        # There are logic claims implicit in the fall through
        # Common case first, leader's idea of cluster state matches ours, no new records
        # for the log, a heartbeat, in other words
        if message.term == self.term and message.prevLogIndex == self.commit_index:
            await self.send_append_entries_response(message, None)
            return
        # Very rare case, sender thinks it is leader but has old term, probably
        # a network partition, or some kind of latency problem with the claimant's
        # operations that made us have an election. Tell the sender it is not leader any more.
        if message.term < self.term:
            await self.send_reject_append_response(message)
            return
        self.last_leader_contact = time.time()
        # Rare case, election in progress and leader declaring itself winner, other
        # append entries logic the same as when local and leader have same term
        if message.term > self.term:
            self.logger.info("accepting leader %s after voting for %s", message.sender,
                             self.last_vote)
            self.leader_uri = message.sender
            self.last_vote = None
            if message.prevLogIndex == self.commit_index:
                # no new records
                self.logger.info("no new records, just election result")
                await self.send_append_entries_response(message, None)
                return
        # We know messag.term == term from first if test seive.
        # We know message.prevLogIndex is > self.commit_index
        # because protocol guarantees it is not less (if code is correct)
        # because then we would be the leader, or some other server would
        # be, and the term would be wrong and we would reject this message above.
        # We know index is not equal cause of two checks above in first and
        # third if statement clauses
        raise Exception('handle new entries here')
        await self.send_append_entries_response(message, None)
        return RaftContext()

    async def on_vote_request(self, message):
        if self.last_vote is not None:
            # we only vote once
            await self.send_vote_response_message(message, votedYes=False)
            return
        # Leadership claims have to be for max log commit index of
        # at least the same as our local copy
        last_index = self.log.get_commit_index()
        # If the messages claim for log index or term are not at least as high
        # as our local values, then vote no.
        if message.prevLogIndex < last_index or message.term < self.log.get_term():
            self.logger.info("voting false on message %s %s",
                             message, message.data)
            self.logger.info("my last vote = %s, index %d, last term %d",
                             self.last_vote, last_index, last_term)
            vote = False
        else: # both term and index proposals are acceptable, so vote yes
            self.last_vote = message.sender
            self.logger.info("voting true for candidate %s", message.sender)
            vote = True
        await self.send_vote_response_message(message, votedYes=vote)
            
    async def term_expired(self, message):
        self.log.set_term(message.term)
        # process the message as normal
        return message

    async def lost_leader(self):
        await self.hull.start_campaign()
        
    async def send_vote_response_message(self, message, votedYes=True):
        vote_response = RequestVoteResponseMessage(sender=self.hull.get_my_uri(),
                                                   receiver=message.sender,
                                                   term=message.term,
                                                   data={"response": votedYes})
        await self.hull.send_response(message, vote_response)
        
    async def send_append_entries_response(self, message, new_records):
        data = dict(success=True,
                    last_index=self.log.get_last_index(),
                    last_term=self.log.get_last_term(),
                    commit_only=True)
        append_response = AppendResponseMessage(sender=self.hull.get_my_uri(),
                                                receiver=message.sender,
                                                term=self.log.get_term(),
                                                data=data,
                                                prevLogIndex=message.prevLogIndex,
                                                prevLogTerm=message.prevLogTerm,
                                                leaderCommit=message.leaderCommit)
        await self.hull.send_response(message, append_response)

    async def contact_checker(self):
        max_time = self.hull.get_leader_lost_timeout()
        if time.time() - self.last_leader_contact > max_time:
            await self.leader_lost()
            return
        # reschedule
        await self.run_after(self.hull.get_leader_lost_timeout(), self.contact_checker)
    
