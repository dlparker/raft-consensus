import asyncio
import random
from raftframe.v2.states.base_state import StateCode, Substate, BaseState
from raftframe.messages.request_vote import RequestVoteMessage
from raftframe.v2.states.context import RaftContext

class Candidate(BaseState):

    def __init__(self, hull):
        super().__init__(hull, StateCode.candidate)
        self.term = self.log.get_term()
        self.votes = dict()
        self.reply_count = 0

    async def start(self):
        await super().start()
        await self.start_campaign()
        
    async def start_campaign(self):
        self.term += 1
        self.reply_count = 0
        self.log.set_term(self.term)
        for node_id in self.hull.get_cluster_node_ids():
            if node_id == self.hull.get_my_uri():
                self.votes[node_id] = True
            else:
                self.votes[node_id] = None
                message = RequestVoteMessage(sender=self.hull.get_my_uri(),
                                             receiver=node_id,
                                             term=self.term,
                                             data="",
                                             prevLogTerm=self.log.get_term(),
                                             prevLogIndex=self.log.get_last_index(),
                                             leaderCommit=0)
                await self.hull.send_message(message)

    
    async def on_vote_response(self, message):
        if message.term < self.term:
            self.logger.info("candidate %s ignoring out of date vote", self.hull.get_my_uri())
            return
        self.votes[message.sender] = message.data['response']
        self.logger.info("candidate %s voting result %s from %s", self.hull.get_my_uri(),
                         message.data['response'], message.sender)
        self.reply_count += 1
        tally = 0
        for nid in self.votes:
            if self.votes[nid] == True:
                tally += 1
        self.logger.info("candidate %s voting results with %d votes in, wins = %d (includes self)",
                         self.hull.get_my_uri(), self.reply_count + 1, tally)
        if tally > len(self.votes) / 2:
            await self.hull.win_vote(self.term)
            return
        if self.reply_count + 1 > len(self.votes) / 2:
            self.logger.info("candidate %s campaign lost, trying again", self.hull.get_my_uri())
            await self.retry()
            return

    async def term_expired(self, message):
        self.log.set_term(message.term)
        await self.hull.demote_and_handle(message)
        # don't reprocess message
        return None

    async def retry(self):
        await self.run_after(self.hull.get_election_timeout(), self.start_campaign)
        


