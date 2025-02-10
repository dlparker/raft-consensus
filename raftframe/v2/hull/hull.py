import traceback
import logging
import random
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
        if self.state:
            await self.state.stop()
        self.state = Candidate(self)
        await self.state.start()
        self.logger.warning("%s started campaign %s", self.get_my_uri(), self.log.get_term())

    async def win_vote(self, new_term):
        if self.state:
            await self.state.stop()
        self.state = Leader(self, new_term)
        self.logger.warning("%s promoting to leader for term %s", self.get_my_uri(), new_term)
        await self.state.start()

    async def demote_and_handle(self, message):
        if self.state:
            self.logger.warning("%s demoting to follower from %s", self.get_my_uri(), self.state)
            await self.state.stop()
        else:
            self.logger.warning("%s demoting to follower", self.get_my_uri())
        # special case where candidate or leader got an append_entries message,
        # which means we need to switch to follower and retry
        self.state = Follower(self)
        await self.state.start()
        if message:
            return await self.on_message(message)

    async def send_message(self, message):
        self.logger.debug("Sending message type %s to %s", message.get_code(), message.receiver)
        await self.config.message_sender(message)

    async def send_response(self, message, response):
        self.logger.debug("Sending response type %s to %s", response.get_code(), response.receiver)
        await self.config.response_sender(message, response)

    async def on_message(self, message):
        self.logger.debug("Handling message type %s", message.get_code())
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
        self.logger.error("%s had error handling message %s", self.get_my_uri(), error)
        
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

    def get_leader_lost_timeout(self):
        return self.config.local.leader_lost_timeout

    def get_election_timeout(self):
        # TODO: needs to change to random.uniform(election_timeout_min, election_timeout_max)
        res = random.uniform(self.config.local.election_timeout_min,
                             self.config.local.election_timeout_max)
        return res
