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
        await self.start_campaign()
        
    async def start_campaign(self):
        self.term += 1
        for node_id in self.hull.get_cluser_node_ids():
            if node_id == self.hull.cluster_node_id:
                votes[node_id] = True
            else:
                votes[node_id] = None
                message = RequestVoteMessage(sender=self.hull.cluster_node_id,
                                             receiver=node_id,
                                             term=self.term,
                                             prevLogTerm=self.log.get_term(),
                                             prevLogIndex=self.log.get_commit_index(),
                                             leaderCommit=0)
                await self.hull.send_message(node_id, message)

    
    async def request_vote_response(self, message):
        self.votes[message.sender] = message.data['response']
        self.reply_count += 1
        tally = 0
        for nid in self.votes:
            if self.votes[nid]:
                tally += 1
        if tally > len(self.votes) / 2:
            await self.hull.win_vote(self.term)
            return
        if self.reply_count + 1 > len(self.votes) / 2:
            await self.retry()
            return

    async def retry(self):
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await self.start_campaign()
        
    async def append_entries(self, message):
        if message.term > self.term:
            await self.resign(message)
        else:
            await self.hull.send_reject_append_response(message)

    async def resign(self, message):
        return await self.hull.demote_and_handle(message)


