import logging
import traceback
from enum import Enum

from raft.states.base_state import State, Substate
from raft.states.state_map import StandardStateMap
from raft.app_api.app import StateChangeMonitor
from raft.comms.memory_comms import MessageInterceptor

from raft.tests.bt_server import MemoryBankTellerServer
from raft.tests.timer import get_timer_set

class TriggerType(str, Enum):

    interceptor = "INTERCEPTOR"
    state = "STATE"
    substate = "SUBSTATE"

class PausingMonitor(StateChangeMonitor):

    def __init__(self, server, name, logger):
        self.server = server
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
        self.logger.info(f"{self.name} from {old_state} to {new_state}")
        method = self.pause_on_states.get(str(new_state), None)
        if method:
            try:
                clear = await method(old_state, new_state)
            except:
                traceback.print_exc()
                clear = True
            if clear:
                self.logger.warning("removing state pause for %s",
                                 state)
                del self.pause_on_states[str(state)] 
        self.state_history.append(old_state)
        self.substate_history = []
        self.state = new_state
        self.state_map = state_map
        return new_state

    async def new_substate(self, state_map, state, substate):
        import threading
        this_id = threading.Thread.ident
        self.logger.info(f"{self.name} {state} to substate {substate}")
        method = self.pause_on_substates.get(substate, None)
        if method:
            try:
                self.logger.info(f"{self.name} calling substate method")
                clear = await method(self.state, self.substate, substate)
            except:
                traceback.print_exc()
                clear = True
            if clear:
                self.logger.warning("removing substate pause from %s to %s",
                                 self.state.substate, substate)
                del self.pause_on_substates[substate] 
        self.substate_history.append(self.substate)
        self.substate = substate
        self.state_map = state_map

    def set_pause_on_state(self, state: State, method=None):
        if method is None:
            method = self.state_pause_method
        self.pause_on_states[str(state)] = method
        
    async def state_pause_method(self, old_state, new_state):
        await self.server.pause_all(TriggerType.state,
                                    dict(old_state=old_state,
                                         new_state=new_state))
        
    async def substate_pause_method(self, state, old_substate, new_substate):
        await self.server.pause_all(TriggerType.substate,
                                    dict(state=state,
                                         old_substate=old_substate,
                                         new_substate=new_substate))
        
        
    def set_pause_on_substate(self, substate: Substate, method=None):
        if method is None:
            method = self.substate_pause_method
        self.pause_on_substates[substate] = method

class InterceptorMode(str, Enum):
    in_before = "IN_BEFORE"
    out_before = "OUT_BEFORE"
    in_after = "IN_AFTER"
    out_after = "OUT_AFTER"

class PausingInterceptor(MessageInterceptor):

    def __init__(self, server, logger):
        self.server = server
        self.logger = logger
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}
        
    async def before_in_msg(self, message) -> bool:
        method = self.in_befores.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("Before message %s in calling method",
                              message.code)
            go_on = await method(InterceptorMode.in_before,
                           message.code,
                           message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            del self.in_befores[message.code]
        return go_on

    async def after_in_msg(self, message) -> bool:
        method = self.in_afters.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("After message %s in calling method",
                              message.code)
            go_on = await method(InterceptorMode.in_after,
                                 message.code,
                                 message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            del self.in_afters[message.code]
        return go_on

    async def before_out_msg(self, message) -> bool:
        method = self.out_befores.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("Before message %s out calling method",
                             message.code)
            go_on = await method(InterceptorMode.out_before,
                                 message.code,
                                 message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            del self.out_befores[message.code]
        return go_on

    async def after_out_msg(self, message) -> bool:
        method = self.out_afters.get(message.code, None)
        go_on = True
        if not method:
            return go_on
        try:
            self.logger.info("After message %s out calling method",
                             message.code)
            go_on = await method(InterceptorMode.out_after,
                                 message.code,
                                 message)
        except:
            self.logger.error("Clearing interceptor because exception %s",
                              traceback.format_exc())
            del self.out_afters[message.code]
        return go_on

    async def pause_method(self, mode, code, message):
        await self.server.pause_all(TriggerType.interceptor,
                                    dict(mode=mode,
                                         code=code))
        return True
    
    def clear_triggers(self):
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}

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
                 log_config=None, vote_at_start=True):
        super().__init__(port, working_dir, name, others,
                         log_config, vote_at_start)
        self.logger = logging.getLogger(__name__)
        self.state_map = StandardStateMap()
        self.interceptor = PausingInterceptor(self, self.logger)
        self.comms.set_interceptor(self.interceptor)
        self.monitor = PausingMonitor(self, f"{port}", self.logger)
        self.state_map.add_state_change_monitor(self.monitor)
        self.timer_set = get_timer_set()
        self.paused = False

    async def pause_all(self, trigger_type, trigger_data):
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
            
        await self.timer_set.pause_all_this_thread()
        self.comms.pause()
        self.paused = True
        self.logger.info("%s paused all timers this thread and comms",
                         self.port)
        

    async def resume_all(self):
        await self.timer_set.resume_all_this_thread()
        self.comms.resume()
        self.paused = False
        self.logger.info("%s resumed", self.port)
