import asyncio
import logging
import traceback
from collections import defaultdict
from raft.servers.server import Server
from raft.states.base_state import State
from raft.states.base_state import Substate
from raft.states.follower import Follower
from raft.states.candidate import Candidate
from raft.states.leader import Leader
from raft.states.state_map import StandardStateMap
from raft.log.memory_log import MemoryLog

# Wrapper classes that do useful test support things
# when running in single proc, threaded, MemoryComms
# based mode.
class ServerWrapper(Server):
    pass

class StateCST:

    def __init__(self, state, superstate, logger):
        self.state = state
        self.superstate = superstate
        self.logger = logger
        self.pause_before_on_message = None
        self.pause_after_on_message = None
        self.pause_on_substates = {}

    def clear_pauses(self):
        self.pause_before_on_message = None
        self.pause_after_on_message = None
        self.pause_on_substates = {}

    def set_pause_before_on_message(self, method):
        self.pause_before_on_message = method

    def set_pause_after_on_message(self, method):
        self.pause_after_on_message = method

    def set_pause_on_substate(self, substate: Substate, method):
        self.pause_on_substates[substate] = method

    async def set_substate(self, substate: Substate):
        if substate != self.state.substate:
            self.logger.info("Moving substate from %s to %s",
                             self.state.substate, substate)
            await self.superstate.set_substate(substate)
        method = self.pause_on_substates.get(substate, None)
        if method:
            try:
                clear = await method(self.state, substate)
            except:
                traceback.print_exc()
                clear = True
            if clear:
                self.logger.info("removing substate pause from %s to %s",
                                 self.state.substate, substate)
                del self.pause_on_substates[substate] 
            

    async def on_message(self, message):
        if self.pause_before_on_message:
            try:
                clear = await self.pause_before_on_message(self.state, message)
            except:
                traceback.print_exc()
                clear = True
            if clear:
                self.pause_before_on_message = None
        res = await self.superstate.on_message(message)
        if self.pause_after_on_message:
            try:
                self.logger.info("%s pausing after on message %s",
                                 self.state, message.code)
                clear = await self.pause_after_on_message(self.state, message)
            except: 
                traceback.print_exc()
                clear = True
            if clear:
                self.pause_after_on_message = None
        return res
    
class FollowerWrapper(Follower):

    def __init__(self, *args, **kwargs):
        vote_at_start = kwargs.pop("vote_at_start")
        self.server_wrapper = kwargs.pop("server_wrapper")
        super().__init__(**kwargs)
        self.wrapper_logger = logging.getLogger("FollowerWrapper")
        self.cst = StateCST(self, super(), self.wrapper_logger)
        # If vote_at_start flag is true, start and election
        # immediately so tests don't have to wait for timeout.
        if vote_at_start:
            asyncio.get_event_loop().call_soon(self.leader_lost)

    def clear_pauses(self):
        self.cst.clear_pauses()
        
    def set_pause_before_on_message(self, method):
        self.cst.set_pause_before_on_message(method)

    def set_pause_after_on_message(self, method):
        self.cst.set_pause_after_on_message(method)

    def set_pause_on_substate(self, substate, method):
        self.cst.set_pause_on_substate(substate, method)

    async def set_substate(self, substate: Substate):
        await self.cst.set_substate(substate)

    async def on_message(self, message):
        return await self.cst.on_message(message)

    
class CandidateWrapper(Candidate):

    def __init__(self, *args, **kwargs):
        self.server_wrapper = kwargs.pop("server_wrapper")
        super().__init__(*args, **kwargs)
        self.wrapper_logger = logging.getLogger("CandidateWrapper")
        self.cst = StateCST(self, super(), self.wrapper_logger)

    def clear_pauses(self):
        self.cst.clear_pauses()
        
    def set_pause_before_on_message(self, method):
        self.cst.set_pause_before_on_message(method)

    def set_pause_after_on_message(self, method):
        self.cst.set_pause_after_on_message(method)

    def set_pause_on_substate(self, substate, method):
        self.cst.set_pause_on_substate(substate, method)

    async def set_substate(self, substate: Substate):
        await self.cst.set_substate(substate)

    async def on_message(self, message):
        return await self.cst.on_message(message)

class LeaderWrapper(Leader):

    def __init__(self, *args, **kwargs):
        self.server_wrapper = kwargs.pop("server_wrapper")
        super().__init__(*args, **kwargs)
        self.wrapper_logger = logging.getLogger("LeaderWrapper")
        self.cst = StateCST(self, super(), self.wrapper_logger)

    def clear_pauses(self):
        self.cst.clear_pauses()
        
    def set_pause_before_on_message(self, method):
        self.cst.set_pause_before_on_message(method)

    def set_pause_after_on_message(self, method):
        self.cst.set_pause_after_on_message(method)

    def set_pause_on_substate(self, substate, method):
        self.cst.set_pause_on_substate(substate, method)

    async def set_substate(self, substate: Substate):
        await self.cst.set_substate(substate)

    async def on_message(self, message):
        return await self.cst.on_message(message)

    async def break_on_vote_received(self, message): # pragma: no cover error
        reason = "Unexpectedly got vote received"
        self.logger.warning(reason)
        try:
            await self.server_wrapper.pause_on_reason(reason)
        except Exception as e:
            print(e)
            print(self.server)
            print(dir(self.server))
            raise
        
    async def break_on_heartbeat(self, message): # pragma: no cover error
        reason = "Why am I getting hearbeat when I am leader?"
        self.logger.warning(reason)
        try:
            await self.server_wrapper.pause_on_reason(reason)
        except Exception as e:
            print(e)
            print(self.server)
            print(dir(self.server))
            raise

class LogWrapper(MemoryLog):
    pass

class StandardStateMapWrapper(StandardStateMap):

    def __init__(self, *args, **kwargs):
        vote_at_start = kwargs.get("vote_at_start", None)
        if vote_at_start:
            kwargs.pop("vote_at_start")
        super().__init__(*args, **kwargs)
        self.first_time = True
        self.state = None
        self.vote_at_start = vote_at_start
        self.pause_methods = defaultdict(dict)
        self.state_names = ['follower', 'candidate', 'leader']
        self.election_counter = 0
        self.server_wrapper = None
        
    def set_server_wrapper(self, wrapper):
        self.server_wrapper = wrapper
        
    def clear_pauses(self):
        if self.state:
            self.state.clear_pauses()
        
    def set_pause_on_substate(self, state_name, substate_name, method):
        if state_name != "all":
            self.pause_methods[state_name][substate_name] = method
            return
        for name in self.state_names:
            methrec = self.pause_methods[name]
            methrec[substate_name] = method
        if self.state:
            if str(self.state) == state_name or state_name == "all":
                self.state.set_on_substate(substate_name, method)
            
    def set_pause_before_on_message(self, state_name, method):
        if state_name != "all":
            self.pause_methods[state_name]["before_on_message"] = method
            return
        for name in self.state_names:
            methrec = self.pause_methods[name]
            methrec['before_on_message'] = method
        if self.state:
            if str(self.state) == state_name or state_name == "all":
                self.state.set_before_after_on_message(method)

    def set_pause_after_on_message(self, state_name, method):
        if state_name != "all":
            self.pause_methods[state_name]["after_on_message"] = method
            return
        for name in self.state_names:
            methrec = self.pause_methods[name]
            methrec['after_on_message'] = method
        if self.state:
            if str(self.state) == state_name or state_name == "all":
                self.state.set_pause_after_on_message(method)

    def set_state(self, state):
        self.state = state
        for sname,pauses in self.pause_methods.items():
            for name, method in pauses.items():
                if name == "before_on_message":
                    state.set_pause_before_on_message(method)
                elif name == "after_on_message":
                    state.set_pause_after_on_message(method)
                else:
                    state.set_pause_on_substate(name, method)
        
    async def switch_to_follower(self, old_state=None):
        # The vote_at_start flag causes the follower
        # to start an election immediately so that
        # tests run faster. We only use this the
        # first time that a Follower is created
        # so later the normal timeouts will decide
        # when an election is needed.
        if self.vote_at_start is not None and self.vote_at_start:
            vote = self.first_time
        else:
            vote = False
        self.logger.info("switching state from %s to follower", self.state)
        follower = FollowerWrapper(server=self.server,
                                   timeout=0.5,
                                   vote_at_start=False,
                                   server_wrapper=self.server_wrapper)
        self.first_time = False
        self.server.set_state(follower)
        self.set_state(follower)
        return follower

    async def switch_to_candidate(self, old_state=None):
        # The vote_at_start flag causes the follower
        # to start an election immediately so that
        # tests run faster. We only use this the
        # first time that a Follower is created
        # so later the normal timeouts will decide
        # when an election is needed.
        self.election_counter += 1
        self.logger.info("switching state from %s to candidate", self.state)
        candidate = CandidateWrapper(server=self.server,
                                     timeout=0.5,
                                     server_wrapper=self.server_wrapper)
        self.server.set_state(candidate)
        self.set_state(candidate)
        return candidate

    async def switch_to_leader(self, old_state=None):
        # The vote_at_start flag causes the follower
        # to start an election immediately so that
        # tests run faster. We only use this the
        # first time that a Follower is created
        # so later the normal timeouts will decide
        # when an election is needed.
        self.election_counter = 0
        self.logger.info("switching state from %s to leader", self.state)
        leader = LeaderWrapper(server=self.server,
                               heartbeat_timeout=0.5,
                               server_wrapper=self.server_wrapper)

        self.server.set_state(leader)
        self.set_state(leader)
        return leader
    
