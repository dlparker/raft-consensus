import asyncio
import traceback
import logging
import random
from raftframe.messages.base_message import BaseMessage
from raftframe.states.base_state import StateCode, BaseState
from raftframe.states.follower import Follower
from raftframe.states.candidate import Candidate
from raftframe.states.leader import Leader
from raftframe.hull.api import PilotAPI

class Hull:

    def __init__(self, cluster_config, local_config, pilot: PilotAPI):
        self.cluster_config = cluster_config
        self.local_config = local_config
        if not isinstance(pilot, PilotAPI):
            raise Exception('Must supply a raftframe.hull.api.PilotAPI implementation')
        self.pilot = pilot
        self.log = pilot.get_log()
        self.state = BaseState(self, StateCode.paused)
        self.logger = logging.getLogger("Hull")
        self.state_async_handle = None
        self.state_run_after_target = None
        self.message_problem_history = []

    async def start(self):
        self.state = Follower(self)
        await self.state.start()

    async def stop_state(self):
        await self.state.stop()
        if self.state_async_handle:
            self.logger.debug("%s canceling scheduled task", self.get_my_uri())
            self.state_async_handle.cancel()
            self.state_async_handle = None
                
    async def start_campaign(self):
        await self.stop_state()
        self.state = Candidate(self)
        await self.state.start()
        self.logger.warning("%s started campaign %s", self.get_my_uri(), await self.log.get_term())

    async def win_vote(self, new_term):
        await self.stop_state()
        self.state = Leader(self, new_term)
        self.logger.warning("%s promoting to leader for term %s", self.get_my_uri(), new_term)
        await self.state.start()

    async def demote_and_handle(self, message=None):
        self.logger.warning("%s demoting to follower from %s", self.get_my_uri(), self.state)
        await self.stop_state()
        # special case where candidate or leader got an append_entries message,
        # which means we need to switch to follower and retry
        self.state = Follower(self)
        await self.state.start()
        if message:
            return await self.on_message(message)

    async def send_message(self, message):
        self.logger.debug("Sending message type %s to %s", message.get_code(), message.receiver)
        await self.pilot.send_message(message.receiver, message)

    async def send_response(self, message, response):
        self.logger.debug("Sending response type %s to %s", response.get_code(), response.receiver)
        await self.pilot.send_response(response.receiver, message, response)

    async def on_message(self, message):
        res = None
        try:
            if not isinstance(message, BaseMessage):
                raise Exception('Message is not a raft type, did you use provided deserializer?')
            self.logger.debug("Handling message type %s", message.get_code())
            res = await self.state.on_message(message)
        except Exception as e:
            error = traceback.format_exc()
            self.logger.error(error)
            await self.record_message_problem(message, error)
            return None
        return res

    async def apply_command(self, command):
        if self.state.state_code == StateCode.leader:
            result = await self.state.apply_command(command)
            return dict(result=result, retry=None, redirect=None)
        elif self.state.state_code == StateCode.follower:
            return dict(result=None, retry=None, redirect=self.state.leader_uri)
        elif self.state.state_code == StateCode.candidate:
            return dict(result=None, retry=1, redirect=None)

    async def state_after_runner(self, target):
        if self.state.stopped:
            return
        await target()
        
    async def state_run_after(self, delay, target):
        loop = asyncio.get_event_loop()
        if self.state_async_handle:
            self.logger.debug('%s cancelling after target to %s', self.local_config.uri,
                        self.state_run_after_target)
            self.state_async_handle.cancel()
        self.logger.debug('%s setting run after target to %s', self.local_config.uri, target)
        self.state_run_after_target = target
        self.state_async_handle = loop.call_later(delay,
                                                  lambda target=target:
                                                  asyncio.create_task(self.state_after_runner(target)))

    async def cancel_state_run_after(self):
        if self.state_async_handle:
            self.state_async_handle.cancel()
            self.state_async_handle = None
        
    async def record_message_problem(self, message, problem):
        rec = dict(problem=problem, message=message)
        self.message_problem_history.append(rec)

    def get_log(self):
        return self.log

    def get_state_code(self):
        return self.state.state_code

    def get_my_uri(self):
        return self.local_config.uri
        
    def get_processor(self):
        return self.pilot
    
    async def get_term(self):
        return await self.log.get_term()

    def get_cluster_node_ids(self):
        return self.cluster_config.node_uris

    def get_leader_lost_timeout(self):
        return self.cluster_config.leader_lost_timeout

    def get_heartbeat_period(self):
        return self.cluster_config.heartbeat_period

    def get_election_timeout(self):
        res = random.uniform(self.cluster_config.election_timeout_min,
                             self.cluster_config.election_timeout_max)
        return res
