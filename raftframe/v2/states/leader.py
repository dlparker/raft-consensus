from raftframe.v2.states.base_state import StateCode, Substate, BaseState
from raftframe.v2.states.context import RaftContext

class Leader(BaseState):

    def __init__(self, hull):
        super().__init__(hull, StateCode.leader)
        
    async def append_entries_response(self, message):
        print('in append_entries_response')
        return RaftContext()




