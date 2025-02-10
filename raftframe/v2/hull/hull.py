import traceback
import logging
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
        self.logger = logging.getLogger(self.__class__.__name__)

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

    async def send_message(self, message):
        self.logger.info("Sending message type %s to %s", message.get_code(), message.receiver)
        await self.config.message_sender(message)

    async def send_response(self, message, response):
        self.logger.info("Sending response type %s to %s", message.get_code(), message.receiver)
        await self.config.response_sender(message, response)

    async def on_message(self, message):
        self.logger.info("Handling message type %s", message.get_code())
        if not isinstance(message, BaseMessage):
            raise Exception('Message is not a raft type, did you use provided deserializer?')
        try:
            res = await self.state.on_message(message)
        except Exception as e:
            error = traceback.format_exc()
            self.logger.error(error)
            await self.handle_message_error(message, error)
            return None
        if isinstance(res, RaftContext):
            return res

    async def handle_message_error(self, message, error):
        print(error)
        
    def get_log(self):
        return self.log

    def get_state_code(self):
        return self.state.state_code

    def get_my_uri(self):
        return self.config.local.uri
        
    def get_term(self):
        return self.log.get_term()

    def get_cluster_node_ids(self):
        return self.config.cluster.node_uris

