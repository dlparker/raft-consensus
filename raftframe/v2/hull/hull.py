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
        self.response_sender = config.response_sender
        self.message_sender = config.message_sender
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

    async def start(self):
        self.state = Follower(self)
        await self.state.start()

    async def start_campaign(self):
        self.state = Candidate(self)
        await self.state.start()

    async def win_vote(self, new_term):
        self.state = Leader(self, new_term)
        await self.state.start()

    async def demote_and_handle(self, message):
        # special case where candidate got an append_entries message,
        # which means we need to switch to follower and retry
        self.state = Follower(self)
        if message:
            return await self.on_message(message)

    async def send_response(self, message, response):
        raise Exception('not implemented')

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

    async def handle_message_error(self, message, error):
        print(error)
        
