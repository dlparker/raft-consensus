from raftframe.v2.states.base_state import StateCode, Substate, BaseState
from raftframe.v2.states.context import RaftContext

class Candidate(BaseState):

    def __init__(self, hull):
        super().__init__(hull, StateCode.candidate)
        
    async def append_entries(self, message):
        await self.resign()
        return await self.hull.demote_and_handle(message)

    async def resign(self):
        pass


