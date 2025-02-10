import logging
from raftframe.v2.states.base_state import StateCode, Substate, BaseState
from raftframe.v2.states.context import RaftContext
from raftframe.messages.append_entries import AppendEntriesMessage

class Leader(BaseState):

    def __init__(self, hull, term):
        super().__init__(hull, StateCode.leader)
        self.term = term
        self.pending_commit = dict()

    async def start(self):
        await self.send_entries()

    async def send_entries(self):
        tracker = dict()
        for nid in self.hull.get_cluster_node_ids():
            if nid == self.hull.get_my_uri():
                continue
            tracker[nid] = "sent"
            message = AppendEntriesMessage(sender=self.hull.get_my_uri(),
                                           receiver=nid,
                                           term=self.term,
                                           data=[],
                                           prevLogTerm=self.log.get_term(),
                                           prevLogIndex=self.log.get_commit_index(),
                                           leaderCommit=True)
            self.logger.info("sending append_entries to %s", nid)
            await self.hull.send_message(message)
        self.pending_commit[self.log.get_commit_index()] = tracker
        
    async def append_entries_response(self, message):
        tracker = self.pending_commit.get(message.prevLogIndex, None)
        if tracker is None:
            return
        tracker[message.sender] = "acked"
        acked = 0
        for nid in self.hull.get_cluster_node_ids():
            if nid == self.hull.get_my_uri():
                continue
            if tracker[nid] == "acked":
                acked += 1
        if acked > len(self.hull.get_cluster_node_ids()) + 1: # we count too
            self.logger.info('got consensus on index %d, committing',
                             message.prevLogIndex)
            del self.pending_commit[message.prevLogIndex]
        





