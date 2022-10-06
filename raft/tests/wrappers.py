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
    
    def set_pause_before_on_message(self, method):
        self.pause_before_on_message = method

    def set_pause_after_on_message(self, method):
        self.pause_after_on_message = method

    def set_pause_on_substate(self, substate: Substate, method):
        self.pause_on_substates[substate] = method

    def set_substate(self, substate: Substate):
        if substate != self.state.substate:
            self.logger.info("Moving substate from %s to %s",
                             self.state.substate, substate)
            self.superstate.set_substate(substate)
        method = self.pause_on_substates.get(substate, None)
        if method:
            try:
                clear = method(self.state, substate)
            except:
                traceback.print_exc()
                clear = True
            if clear:
                del self.pause_on_substates[substate] 
            

    def on_message(self, message):
        if self.pause_before_on_message:
            try:
                clear = self.pause_before_on_message(self.state, message)
            except:
                traceback.print_exc()
                clear = True
            if clear:
                self.pause_before_on_message = None
        res = self.superstate.on_message(message)
        if self.pause_after_on_message:
            try:
                clear = self.pause_after_on_message(self.state, message)
            except: 
                traceback.print_exc()
                clear = True
            if clear:
                self.pause_after_on_message = None
        return res
    
class FollowerWrapper(Follower):

    def __init__(self, server, timeout=0.75, vote_at_start=False):
        super().__init__(server, timeout)
        self.wrapper_logger = logging.getLogger("FollowerWrapper")
        self.cst = StateCST(self, super(), self.wrapper_logger)
        # If vote_at_start flag is true, start and election
        # immediately so tests don't have to wait for timeout.
        if vote_at_start:
            asyncio.get_event_loop().call_soon(self.start_election)

    def set_pause_before_on_message(self, method):
        self.cst.set_pause_before_on_message(method)

    def set_pause_after_on_message(self, method):
        self.cst.set_pause_after_on_message(method)

    def set_pause_on_substate(self, substate, method):
        self.cst.set_pause_on_substate(substate, method)

    def set_substate(self, substate: Substate):
        self.cst.set_substate(substate)

    def on_message(self, message):
        self.cst.on_message(message)

    
class CandidateWrapper(Candidate):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wrapper_logger = logging.getLogger("CandidateWrapper")
        self.cst = StateCST(self, super(), self.wrapper_logger)

    def set_pause_before_on_message(self, method):
        self.cst.set_pause_before_on_message(method)

    def set_pause_after_on_message(self, method):
        self.cst.set_pause_after_on_message(method)

    def set_pause_on_substate(self, substate, method):
        self.cst.set_pause_on_substate(substate, method)

    def set_substate(self, substate: Substate):
        self.cst.set_substate(substate)

    def on_message(self, message):
        self.cst.on_message(message)

class LeaderWrapper(Leader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wrapper_logger = logging.getLogger("LeaderWrapper")
        self.cst = StateCST(self, super(), self.wrapper_logger)

    def set_pause_before_on_message(self, method):
        self.cst.set_pause_before_on_message(method)

    def set_pause_after_on_message(self, method):
        self.cst.set_pause_after_on_message(method)

    def set_pause_on_substate(self, substate, method):
        self.cst.set_pause_on_substate(substate, method)

    def set_substate(self, substate: Substate):
        self.cst.set_substate(substate)

    def on_message(self, message):
        self.cst.on_message(message)


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
        
    def set_pause_on_substate(self, state_name, substate_name, method):
        if state_name != "all":
            self.pause_methods[state_name][substate_name] = method
            return
        for name in self.state_names:
            methrec = self.pause_methods[name]
            methrec[substate_name] = method
            
    def set_pause_before_on_message(self, state_name, method):
        if state_name != "all":
            self.pause_methods[state_name]["before_on_message"] = method
            return
        for name in self.state_names:
            methrec = self.pause_methods[name]
            methrec['before_on_message'] = method

    def set_pause_after_on_message(self, state_name, method):
        if state_name != "all":
            self.pause_methods[state_name]["after_on_message"] = method
            return
        for name in self.state_names:
            methrec = self.pause_methods[name]
            methrec['after_on_message'] = method

    def set_state(self, state):
        for sname,pauses in self.pause_methods.items():
            for name, method in pauses.items():
                if name == "before_on_message":
                    state.set_pause_before_on_message(method)
                elif name == "after_on_message":
                    state.set_pause_after_on_message(method)
                else:
                    state.set_pause_on_substate(name, method)

    def switch_to_follower(self, old_state=None):
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
        follower = FollowerWrapper(server=self.server,
                                   vote_at_start=False)
        self.first_time = False
        self.server.set_state(follower)
        self.set_state(follower)
        return follower

    def switch_to_candidate(self, old_state=None):
        # The vote_at_start flag causes the follower
        # to start an election immediately so that
        # tests run faster. We only use this the
        # first time that a Follower is created
        # so later the normal timeouts will decide
        # when an election is needed.
        candidate = CandidateWrapper(server=self.server,
                                    timeout=0.5)
        self.server.set_state(candidate)
        self.set_state(candidate)
        return candidate

    def switch_to_leader(self, old_state=None):
        # The vote_at_start flag causes the follower
        # to start an election immediately so that
        # tests run faster. We only use this the
        # first time that a Follower is created
        # so later the normal timeouts will decide
        # when an election is needed.
        leader = LeaderWrapper(server=self.server,
                               heartbeat_timeout=0.5)
        self.server.set_state(leader)
        self.set_state(leader)
        return leader
    
