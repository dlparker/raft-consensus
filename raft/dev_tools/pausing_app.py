import os
import logging
import time
import asyncio
import traceback
import threading
from enum import Enum
from typing import Union
from raft.states.base_state import State, Substate
from raft.states.follower import Follower
from raft.states.candidate import Candidate
from raft.states.leader import Leader
from raft.states.state_map import StandardStateMap, StateMap
from raft.app_api.app import StateChangeMonitor
from raft.comms.memory_comms import MessageInterceptor

from raft.dev_tools.bt_server import MemoryBankTellerServer
from raft.dev_tools.timer_wrapper import get_timer_set

class PFollower(Follower):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.paused = False

    async def leader_lost(self):
        if self.paused:
            self.logger.info("not doing leader lost, paused")
            return
        return await super().leader_lost()

    async def start_election(self):
        if self.paused:
            self.logger.info("\n\n\tnot doing start_election, paused\n\n")
            return
        self.logger.info("doing start_election, not paused")
        return await super().start_election()
        
    async def on_heartbeat(self, message):
        if self.paused:
            self.logger.info("not doing heartbeat, paused")
            return
        return await super().on_heartbeat(message)
        
class PCandidate(Candidate):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.paused = False

    async def on_timer(self):
        if self.paused:
            return
        return await super().on_timer()
        
class PLeader(Leader):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.paused = False


class TriggerType(str, Enum):

    interceptor = "INTERCEPTOR"
    state = "STATE"
    substate = "SUBSTATE"

class PausingMonitor(StateChangeMonitor):

    def __init__(self, pbt_server, name, logger):
        self.pbt_server = pbt_server
        self.name = name
        self.logger = logger
        self.state_map = None
        self.state_history = []
        self.substate_history = []
        self.state = None
        self.substate = None
        self.pause_on_substates = {}
        self.pause_on_states = {}

    async def new_state(self, state_map, old_state, new_state):
        import threading
        this_id = threading.Thread.ident
        if self.pbt_server.paused:
            msg = f"trying {self.name} from {old_state}" \
                f" to {new_state} but should be paused!"
            self.logger.error(msg)
            #raise Exception(msg)
            print(f"\n\n\t{msg}")
            traceback.print_stack()
            print(f"\n\tsuiciding\n\n")
            os.system(f"kill {os.getpid()}")

        self.logger.info(f"{self.name} from {old_state} to {new_state}")
        self.state_history.append(old_state)
        self.substate_history = []
        if new_state._type == "follower":
            new_state = PFollower(new_state.server, new_state.timeout,
                                  new_state.use_log_pull)
        elif new_state._type == "candidate":
            new_state = PCandidate(new_state.server,
                                   new_state.election_timeout)
        elif new_state._type == "leader":
            new_state = PLeader(new_state.server,
                                new_state.heartbeat_timeout,
                                new_state.use_log_pull)
        self.state = new_state
        self.state_map = state_map
        method = self.pause_on_states.get(str(new_state), None)
        if method:
            try:
                # "self" becomes "monitor" arg, in case user
                # supplied method needs context
                clear = await method(self, old_state, new_state)
            except GeneratorExit:
                raise
            except:
                self.logger.error(traceback.format_exc())
                clear = True
                self.logger.warning("removing state pause for %s",
                                    new_state)
            if clear and state in self.pause_on_states:
                del self.pause_on_states[str(state)] 
        return new_state

    async def new_substate(self, state_map, state, substate):
        import threading
        this_id = threading.Thread.ident
        if self.pbt_server.paused:
            msg = f"trying {self.name} {state} to" \
                f" substate {substate} but should be paused!"
            self.logger.error(msg)
            #raise Exception(msg)
            print(f"\n\n\t{msg}")
            traceback.print_stack()
            print(f"\nthread is {threading.get_ident()}\n");
            self.logger.debug(f"\n Timers \n")
            print(f"\n\tsuiciding\n\n")
            os.system(f"kill {os.getpid()}")

        self.substate_history.append(self.substate)
        old_substate = self.substate
        self.substate = substate
        self.state_map = state_map
        method = self.pause_on_substates.get(substate, None)
        self.logger.debug("state %s substate from %s to %s, method %s",
                          state, old_substate, substate, method)
        if method:
            try:
                self.logger.info(f"{self.name} calling substate method")
                # "self" becomes "monitor" arg, in case user
                # supplied method needs context
                clear = await method(self, self.state, old_substate, substate)
            except GeneratorExit:
                raise
            except:
                self.logger.error(traceback.format_exc())
                clear = True
                self.logger.warning("removing substate pause from  %s",
                                    substate)
            if clear and substate in self.pause_on_substates:
                del self.pause_on_substates[substate] 

    def set_pause_on_state(self, state: State, method=None):
        if method is None:
            method = self.state_pause_method
        self.pause_on_states[str(state)] = method
        
    async def state_pause_method(self, monitor, old_state, new_state):
        # the monitor arg is redundant for this method, but
        # caller supplied methods might need it
        await self.pbt_server.pause_all(TriggerType.state,
                                        dict(old_state=old_state,
                                             new_state=new_state))

    def clear_pause_on_substate(self, state):
        if str(state) in self.pause_on_substates:
            del self.pause_on_states[str(state)]
        
    async def substate_pause_method(self, monitor, state,
                                    old_substate, new_substate):
        # the monitor arg is redundant for this method, but
        # caller supplied methods might need it
        await self.pbt_server.pause_all(TriggerType.substate,
                                        dict(state=state,
                                         old_substate=old_substate,
                                         new_substate=new_substate))
        
        
    def set_pause_on_substate(self, substate: Substate, method=None):
        if method is None:
            method = self.substate_pause_method
        self.pause_on_substates[substate] = method

    def clear_pause_on_substate(self, substate: Substate):
        if substate in self.pause_on_substates:
            del self.pause_on_substates[substate]

    def clear_substate_pauses(self):
        self.pause_on_substates = {}

    def clear_state_pauses(self):
        self.pause_on_states = {}
            

class InterceptorMode(str, Enum):
    in_before = "IN_BEFORE"
    out_before = "OUT_BEFORE"
    in_after = "IN_AFTER"
    out_after = "OUT_AFTER"

class PausingInterceptor(MessageInterceptor):

    def __init__(self, pbt_server, logger):
        self.pbt_server = pbt_server
        self.logger = logger
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}
        self.pausing_message = None
        
    async def before_in_msg(self, message) -> bool:
        method = self.in_befores.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("Before message %s in calling method",
                              message.code)
            self.pausing_message = message
            go_on = await method(InterceptorMode.in_before,
                           message.code,
                           message)
            self.pausing_message = None
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            try:
                del self.in_befores[message.code]
            except KeyError:
                # might have been cleared by test code
                pass
        self.pausing_message = None
        return go_on

    async def after_in_msg(self, message) -> bool:
        method = self.in_afters.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("After message %s in calling method",
                              message.code)
            self.pausing_message = message
            go_on = await method(InterceptorMode.in_after,
                                 message.code,
                                 message)
            self.pausing_message = None
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            del self.in_afters[message.code]
        self.pausing_message = None
        return go_on

    async def before_out_msg(self, message) -> bool:
        method = self.out_befores.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("Before message %s out calling method",
                             message.code)
            self.pausing_message = message
            go_on = await method(InterceptorMode.out_before,
                                 message.code,
                                 message)
            self.pausing_message = None
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            del self.out_befores[message.code]
        self.pausing_message = None
        return go_on

    async def after_out_msg(self, message) -> bool:
        method = self.out_afters.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("After message %s out calling method",
                             message.code)
            self.pausing_message = message
            go_on = await method(InterceptorMode.out_after,
                                 message.code,
                                 message)
            self.pausing_message = None
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            del self.out_afters[message.code]
        self.pausing_message = None
        return go_on

    async def pause_method(self, mode, code, message):
        await self.pbt_server.pause_all(TriggerType.interceptor,
                                        dict(mode=mode,
                                             code=code))
        return True
    
    def clear_triggers(self):
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}

    def clear_trigger(self, mode, message_code):
        if mode == InterceptorMode.in_before:
            if message_code in self.in_befores:
                del self.in_befores[message_code]
        elif mode == InterceptorMode.in_after:
            if message_code in self.in_afters:
                del self.in_afters[message_code]
        elif mode == InterceptorMode.out_before:
            if message_code in self.out_befores:
                del self.out_befores[message_code]
        elif mode == InterceptorMode.out_after:
            if message_code in self.out_afters:
                del self.out_afters[message_code]

    def add_trigger(self, mode, message_code, method=None):
        if method is None:
            method = self.pause_method
        if mode == InterceptorMode.in_before:
            self.in_befores[message_code] = method
        elif mode == InterceptorMode.in_after:
            self.in_afters[message_code] = method
        elif mode == InterceptorMode.out_before:
            self.out_befores[message_code] = method
        elif mode == InterceptorMode.out_after:
            self.out_afters[message_code] = method
        else:
            raise Exception(f"invalid mode {mode}")


class PausingBankTellerServer(MemoryBankTellerServer):

    def __init__(self, port, working_dir, name, others,
                 log_config=None, timeout_basis=1.0, use_log_pull=True):
        super().__init__(port, working_dir, name, others,
                         log_config, timeout_basis, use_log_pull)
        self.logger = logging.getLogger(__name__)
        self.state_map = StandardStateMap(timeout_basis=timeout_basis)
        self.state_map.set_use_log_pull(self.use_log_pull)
        self.interceptor = PausingInterceptor(self, self.logger)
        self.comms.set_interceptor(self.interceptor)
        self.monitor = PausingMonitor(self, f"{port}", self.logger)
        self.state_map.add_state_change_monitor(self.monitor)
        self.paused = False
        self.do_resume = False

    async def pause_all(self, trigger_type, trigger_data):
        timer_set = get_timer_set()
        await timer_set.pause_all()
        self.paused = True
        self.state_map.state.pause = True
        if trigger_type == TriggerType.interceptor:
            self.logger.info("%s pausing all on interceptor %s %s",
                             self.port,
                             trigger_data['mode'],
                             trigger_data['code'])
        elif trigger_type == TriggerType.state:
            self.logger.info("%s pausing all on state change %s %s",
                             self.port,
                             trigger_data['old_state'],
                             trigger_data['new_state'])
        elif trigger_type == TriggerType.substate:
            self.logger.info("%s pausing all on substate change %s %s %s",
                             self.port,
                             trigger_data['state'],
                             trigger_data['old_substate'],
                             trigger_data['new_substate'])
        self.logger.info("%s paused all timers this thread and comms",
                         self.port)
        self.logger.info(">>>>>>> %s %s entering pause loop", self.port,
                         self.state_map.state._type)
        while self.paused:
            try:
                await asyncio.sleep(0.01)
            except asyncio.exceptions.CancelledError:
                pass
        self.logger.info("++++++++ %s %s continuing", self.port,
                         self.state_map.state._type)
            
    async def resume_all(self, wait=True):
        # If you have an interceptor or monitor setup to
        # do a pause and you call this, the timeout
        # logic might not work since the next pause might
        # happen before this code can notice the resume.
        # If that is your situation, call with wait = False
        if not self.paused:
            return
        self.do_resume = True
        if not wait:
            return
        start_time = time.time()
        # don't check paused, it might happen again before we
        # can check, just check to see if the do_resume
        # was cleared, which means resume happened
        while time.time() - start_time < 1 and self.do_resume:
            await asyncio.sleep(0.01)
        if self.do_resume:
            raise Exception('resume did not happen!')

    async def in_loop_check(self):
        if self.do_resume:
            self.paused = False
            timer_set = get_timer_set()
            timer_set.resume_all()
            self.do_resume = False
            self.logger.info("<<<<<<< %s %s resumed", self.port,
                             self.state_map.state._type)
