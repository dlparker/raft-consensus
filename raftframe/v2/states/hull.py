from raftframe.v2.states.base_state import StateCode, BaseState
from raftframe.v2.states.context import RaftContext
from raftframe.messages.base_message import BaseMessage

class Hull:

    def __init__(self, config):
        self.config = config
        self.log = config.log # should be some implementation of the LogAPI
        self.state = BaseState(self, StateCode.paused)

    def get_log(self):
        return self.log

    def get_state_code(self):
        return self.state_code
        
    def get_term(self):
        return self.log.get_term()

    async def on_message(self, message):
        if not isinstance(message, BaseMessage):
            raise Exception('Message is not a raft type, did you use provided deserializer?')
        res = await self.state.on_message(message)
        if isinstance(res, RaftContext):
            return res
        return None

    async def start_logged_change(self, change_data):
        ae = AppendEntriesMessage('foo', 'bar', 1, 'a', 1, 1, False)
        
        
