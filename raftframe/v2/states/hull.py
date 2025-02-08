import traceback
from raftframe.v2.states.base_state import StateCode, BaseState
from raftframe.v2.states.follower import Follower
from raftframe.v2.states.candidate import Candidate
from raftframe.v2.states.leader import Leader
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

    def get_my_uri(self):
        return self.config.uri
        
    def get_term(self):
        return self.log.get_term()

    def start(self):
        self.state = Follower(self)

    def start_campaign(self):
        self.state = Candidate(self)

    def win_vote(self):
        self.state = Leader(self)

    async def demote_and_handle(self, message):
        # special case where candidate got an append_entries message,
        # which means we need to switch to follower and retry
        self.state = Follower(self)
        return await self.on_message(message)
    
    async def on_message(self, message):
        if not isinstance(message, BaseMessage):
            raise Exception('Message is not a raft type, did you use provided deserializer?')
        try:
            res = await self.state.on_message(message)
        except Exception as e:
            error = traceback.format_exc()
            await self.handle_message_error(message, error)
            return None
        if isinstance(res, RaftContext):
            return res

    async def start_logged_change(self, change_data):
        ae = AppendEntriesMessage('foo', 'bar', 1, 'a', 1, 1, False)
        
    async def handle_message_error(self, message, error):
        print(error)
        
