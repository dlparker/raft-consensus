#
import asyncio
import os
from pathlib import Path
import traceback
import logging
import threading
import time
import multiprocessing
from typing import Union
from logging.config import dictConfig
from dataclasses import dataclass, field
from enum import Enum

import raftframe
from raftframe.servers.server_config import LiveConfig, ClusterConfig, LocalConfig
from raftframe.servers.server import Server
from raftframe.states.state_map import StateMap, StateMap
from raftframe.states.base_state import State, Substate
from raftframe.app_api.app import StateChangeMonitorAPI
from raftframe.states.follower import Follower
from raftframe.serializers.msgpack import MsgpackSerializer
from raftframe.serializers.json import JsonSerializer
from dev_tools.bt_client import MemoryBankTellerClient
from dev_tools.bank_app import BankingApp
from dev_tools.timer_wrapper import ControlledTimer, get_timer_set
from dev_tools.memory_log import MemoryLog
from dev_tools.memory_comms import MemoryComms, MessageInterceptor
from dev_tools.monrpc import RPCMonitor
    
class PServer:

    def __init__(self, port, working_dir, name, others,
                 log_config=None, timeout_basis=1.0):
        # log_config is ignored, kept just to match process launching
        # versions to make control code cleaner
        self.host = "localhost"
        self.port = port
        self.working_dir = working_dir
        self.other_nodes = others
        self.name = name
        self.timeout_basis = timeout_basis
        self.endpoint = ('localhost', self.port)
        self.thread_started = False
        self.thread_ident = None
        self.client = None
        self.running = False
        self.paused = False
        self.do_direct_pause = False
        self.pause_callback = None
        self.pause_noted = False
        self.pause_context = None
        self.resume_callback = None
        self.resume_noted = False
        self.run_once_in_thread = []
        self.do_log_stats = False
        self.log_stats = None
        self.do_dump_state = False
        self.do_resume = False
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}
        self.pausing_substates = {}
        self.pausing_states = {}
        self.logger = logging.getLogger(__name__)
        self.state_map = StateMap(timeout_basis=timeout_basis)
        self.data_log = MemoryLog()
        self.comms = MemoryComms()
        self.app = BankingApp()
        self.start_monitor = start_monitor = os.environ.get("RPC_MONITOR", False)
        self.thread = ServerThread(self, self.start_monitor)
        self.thread.name = f"{self.port}"
        self.monitor = PauseSupportMonitor(self)
        self.state_map.add_state_change_monitor(self.monitor)
        self.interceptor = Interceptor(self)
        self.comms.set_interceptor(self.interceptor)

    def get_raftframe_server(self):
        return self.thread.server

    def add_run_in_thread_func(self, func):
        self.run_once_in_thread.append(func)
        
    async def pause_timers(self):
        self.logger.debug("%s, %s pausing timers", self.name, self.thread_ident)
        await self.thread.pause_timers()

    async def resume_timers(self):
        self.logger.debug("%s, %s resuming timers", self.name, self.thread_ident)
        await self.thread.resume_timers()
        
    async def pause_new_messages(self):
        self.logger.debug("%s, %s pausing new messages", self.name, self.thread_ident)
        self.comms.pause_new_messages()

    async def resume_new_messages(self):
        self.logger.debug("%s, %s resuming new messages", self.name, self.thread_ident)
        self.comms.resume_new_messages()

    def direct_pause(self):
        self.do_direct_pause = True
            
    async def pause(self):
        await self.pause_timers()
        await self.pause_new_messages()
        self.paused = True
        self.logger.debug("%s, %s pause signaled to server thread", self.name, self.thread_ident)

    def resume(self):
        if not self.paused:
            return
        self.do_resume = True
        self.logger.debug("%s, %s resume signaled to server thread", self.name, self.thread_ident)

    def pause_on_state(self, state):
        self.logger.debug("%s, %s setting pause on state %s", self.name, self.thread_ident, state)
        self.pausing_states[str(state)] = self.state_pause_method

    def clear_pause_on_state(self, state: State):
        self.logger.debug("%s, %s clearing pause on state %s", self.name, self.thread_ident, state)
        if str(state) in self.pausing_states:
            del self.pausing_states[str(state)]
         
    def clear_state_pauses(self):
        if self.pausing_states == {}:
            return
        self.logger.debug("%s, %s setting pause on all states", self.name, self.thread_ident)
        self.pausing_states = {}

    async def state_pause_method(self, state_map, old_state, new_state):
        await self.pause()
        self.logger.debug("%s, %s starting pause wait loop on state %s", self.name,
                          self.thread_ident, self.pause_context.state)
        while self.paused:
            try:
                await asyncio.sleep(0.001)
            except asyncio.exceptions.CancelledError:
                break

    async def new_state(self, state_map: StateMap,
                        old_state: Union[State, None],
                        new_state: Substate) -> State:
        if str(new_state) in self.pausing_states:
            self.logger.debug("%s, %s pausing on %s", self.name, self.thread_ident, new_state)
            self.pause_context = PauseContext(self, PauseReason.state, state=new_state)
            return await self.pausing_states[str(new_state)](state_map, old_state, new_state)
        return new_state
                
    def pause_on_substate(self, substate):
        self.logger.debug("%s, %s setting pause on substate %s", self.name, self.thread_ident, substate)
        self.pausing_substates[str(substate)] = self.substate_pause_method

    def clear_pause_on_substate(self, substate: Substate):
        self.logger.debug("%s, %s clearing pause on substate %s", self.name, self.thread_ident, substate)
        if str(substate) in self.pausing_substates:
            del self.pausing_substates[str(substate)]

    def clear_substate_pauses(self):
        if self.pausing_substates == {}:
            return
        self.logger.debug("%s, %s setting pause on all substates", self.name, self.thread_ident)
        self.pausing_substates = {}

    async def substate_pause_method(self, state_map,
                                    state, substate):
        await self.pause()
        self.logger.debug("%s, %s starting pause wait loop on substate %s", self.name,
                          self.thread_ident, self.pause_context.substate)
        while self.paused:
            try:
                await asyncio.sleep(0.001)
            except asyncio.exceptions.CancelledError:
                break

    async def new_substate(self, state_map: StateMap,
                            state: State,
                            substate: Substate) -> None:
        if str(substate) in self.pausing_substates:
            self.logger.debug("%s, %s pausing on %s", self.name, self.thread_ident, substate)
            self.pause_context = PauseContext(self, PauseReason.substate, substate=substate)
            old_substate = state_map.substate
            # for our purposes, we want the new substate to already be active
            state_map.substate = substate
            await self.pausing_substates[str(substate)](state_map, state, substate)
            
    async def interceptor_pause_method(self, mode, code, message):
        await self.pause()
        self.logger.debug("%s, %s starting pause wait loop on message %s %s ", self.name,
                          self.thread_ident, self.pause_context.message_code,
                          self.pause_context.reason)
        while self.paused:
            try:
                await asyncio.sleep(0.001)
            except asyncio.exceptions.CancelledError:
                break
        return True
        
    def pause_before_out_message(self, code, intercept_method=None):
        if intercept_method is None:
            intercept_method = self.interceptor_pause_method
        self.logger.debug("%s, %s setting pause before out of %s", self.name, self.thread_ident, code)
        self.out_befores[code] = intercept_method

    def pause_after_out_message(self, code, intercept_method=None):
        if intercept_method is None:
            intercept_method = self.interceptor_pause_method
        self.logger.debug("%s, %s setting pause after out of %s", self.name, self.thread_ident, code)
        self.out_afters[code] = intercept_method
        
    def pause_before_in_message(self, code, intercept_method=None):
        if intercept_method is None:
            intercept_method = self.interceptor_pause_method
        self.logger.debug("%s, %s setting pause before in of %s", self.name, self.thread_ident, code)
        self.in_befores[code] = intercept_method

    def pause_after_in_message(self, code, intercept_method=None):
        if intercept_method is None:
            intercept_method = self.interceptor_pause_method
        self.logger.debug("%s, %s setting pause after in of %s", self.name, self.thread_ident, code)
        self.in_afters[code] = intercept_method

    def clear_message_triggers(self):
        if (self.in_befores != {} 
            or self.in_afters != {}
            or self.out_befores != {} 
            or self.out_afters != {}):
            self.logger.debug("%s, %s clearing all message triggers", self.name, self.thread_ident)
            self.in_befores = {}
            self.in_afters = {}
            self.out_befores = {}
            self.out_afters = {}

    async def before_in_msg(self, message) -> bool:
        if message.code in self.in_befores:
            self.logger.debug("%s, %s pausing before in %s", self.name, self.thread_ident, message.code)
            self.pause_context = PauseContext(self, PauseReason.in_before, message_code=message.code)
            return await self.interceptor_pause_method('IN_BEFORE', message.code, message)
        return True

    async def after_in_msg(self, message) -> bool:
        if message.code in self.in_afters:
            self.logger.debug("%s, %s pausing after in %s", self.name, self.thread_ident, message.code)
            self.pause_context = PauseContext(self, PauseReason.in_after, message_code=message.code)
            return await self.interceptor_pause_method('IN_AFTER', message.code, message)
        return True

    async def before_out_msg(self, message) -> bool:
        if message.code in self.out_befores:
            self.logger.debug("%s, %s pausing before out %s", self.name, self.thread_ident, message.code)
            self.pause_context = PauseContext(self, PauseReason.out_before, message_code=message.code)
            return await self.interceptor_pause_method('OUT_BEFORE', message.code, message)
        return True

    async def after_out_msg(self, message) -> bool:
        if message.code in self.out_afters:
            self.logger.debug("%s, %s pausing after out %s", self.name, self.thread_ident, message.code)
            self.pause_context = PauseContext(self, PauseReason.out_after, message_code=message.code)
            return await self.interceptor_pause_method('OUT_AFTER', message.code, message)
        return True

    def start_thread(self):
        if not self.thread_started:
            self.logger.debug("%s, %s starting server thread but not calling go", self.name, self.thread_ident)
            self.thread.start()
            self.thread_started = True
        return self.thread

    def start(self):
        if not self.thread_started:
            self.logger.debug("%s, %s starting server thread", self.name, self.thread_ident)
            self.thread.start()
            self.thread_started = True
        self.logger.debug("%s, %s calling server thread go", self.name, self.thread_ident)
        self.thread.go()
        self.logger.debug("%s, %s starting monitor rpc thread", self.name, self.thread_ident)

    def configure(self):
        self.thread.configure()
        return self.thread
        
    def stop(self):
        self.thread.stop()
        self.thread.keep_running = False
        self.logger.debug("%s, %s stopped", self.name, self.thread_ident)

    def get_log_stats(self):
        self.do_log_stats = True
        while self.log_stats is None:
            time.sleep(0.001)
        result = self.log_stats
        self.log_stats = None
        return result
    
    async def in_loop_check(self, thread_obj):
        # this is called from the server thread object in that thread

        if self.do_direct_pause:
            # somebody synchronously asked us to pause
            self.do_direct_pause = False
            await self.pause_timers()
            await self.pause_new_messages()
            self.logger.info("Server %s direct pause done\n", self.name)
            self.paused = True
        elif self.paused and not self.pause_noted:
            self.pause_noted = True
            if self.pause_callback:
                await self.pause_callback(self, self.pause_context)
        if self.do_log_stats:
            self.do_log_stats = False
            # cannot get to sqlite log directly from test code because
            # it is not in the right thread
            # so this dodge makes it possible to request it from the
            # test (main) thread and run it in the server thread
            log = thread_obj.server.get_log()
            result = dict(term=log.get_term(),
                          last_term=log.get_last_term(),
                          last_index=log.get_last_index(),
                          commit_index=log.get_commit_index(),
                          last_rec=log.read())
            self.log_stats = result
        if self.do_dump_state:
            self.do_dump_state = False
            state = self.state_map.state
            self.logger.info("Server %s state %s", self.name, state)
            log = thread_obj.server.get_log()
            self.logger.info("Server %s log stats\n"
                                 "term = %d, last_rec_term = %d "\
                                 "last_rec_index = %d, commit = %d",
                                 self.name, 
                                 log.get_term(),
                                 log.get_last_term(),
                                 log.get_last_index(),
                                 log.get_commit_index())
            
        if self.do_resume:
            self.paused = False
            self.pause_noted = False
            self.pause_context = None
            await self.resume_timers()
            await self.resume_new_messages()
            #timer_set = get_timer_set()
            #timer_set.resume_all()
            self.do_resume = False
            self.logger.info("<<<<<<< %s %s resumed", self.port,
                             self.state_map.state)
            if not self.resume_noted:
                self.resume_noted = True
                if self.resume_callback:
                    await self.resume_callback(self)

        while len(self.run_once_in_thread) > 0:
            func = self.run_once_in_thread.pop(0)
            self.logger.debug("%s server thread calling run_once_in_thread %s", self.name,
                              func)
            loop = asyncio.get_running_loop()
            loop.create_task(func)

    def get_client(self):
        if not self.client:
            self.client = MemoryBankTellerClient(*self.endpoint)
        return self.client
            
class ServerThread(threading.Thread):

    def __init__(self, pserver, start_monitor=False):
        threading.Thread.__init__(self)
        self.pserver = pserver
        self.host = "localhost"
        self.ready = False
        self.keep_running = True
        self.running = False
        self.server = None
        self.logger = logging.getLogger(__name__)
        self.rpc_monitor = start_monitor

    async def pause_timers(self):
        await get_timer_set().pause_all()

    async def resume_timers(self):
        get_timer_set().resume_all()

    def run(self):
        self.pserver.thread_ident = threading.get_ident()
        self.pserver.running = True
        if not self.ready:
            self.logger.info("memory comms bank teller server waiting for config")
            while not self.ready:
                time.sleep(0.001)
        if self.server is None:
            self.configure()
        if self.rpc_monitor:
            self.rpc_monitor = RPCMonitor(self.server, self.pserver.port+5000)
            self.rpc_monitor.start()
        self.logger.info("memory comms bank teller server starting")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run())
            try:
                tasks = asyncio.all_tasks()
                for t in [t for t in tasks if not (t.done() or t.cancelled())]:
                    # give canceled tasks the last chance to run
                    loop.run_until_complete(t)
            except RuntimeError:
                pass
        finally:
            loop.close()        
            self.pserver.running = False

    def go(self):
        self.ready = True
        
    def configure(self):
        if self.server:
            return
        
        self.logger.info('creating server')
        cc = ClusterConfig(name=self.pserver.name,
                           endpoint=self.pserver.endpoint,
                           other_nodes=self.pserver.other_nodes)
        local_config = LocalConfig(working_dir=self.pserver.working_dir)
        self.live_config = LiveConfig(cluster=cc,
                                      local=local_config,
                                      app=self.pserver.app,
                                      log=self.pserver.data_log,
                                      comms=self.pserver.comms,
                                      state_map=self.pserver.state_map,
                                      comms_serializer=MsgpackSerializer,
                                      log_serializer=JsonSerializer)
        self.server = Server(live_config=self.live_config)
        self.server.set_timer_class(ControlledTimer)
        self.server.get_endpoint()
        
    async def _run(self):
        self.running = True
        try:
            self.server.start()
            self.logger.info("started server on memory addr %s  with others at %s",
                        self.pserver.endpoint,
                        self.pserver.other_nodes)
            while self.keep_running:
                await asyncio.sleep(0.01)
                await self.pserver.in_loop_check(self)
            self.logger.info("server %s stopping", self.pserver.endpoint)
            await self.pserver.comms.stop()
            await self.server.stop()
        except:
            print("\n\n!!!!!!Server thread failed!!!!")
            traceback.print_exc()
        tasks = asyncio.all_tasks()
        start_time = time.time()
        undone = []
        for t in [t for t in tasks if not (t.done() or t.cancelled())]:
            if t != asyncio.current_task():
                undone.append(t)
        for t in undone:
            self.logger.info("cancelling %s", t)
            t.cancel()
            await asyncio.sleep(0)
        self.running = False

    def stop(self):
        self.keep_running = False
        if self.rpc_monitor:
            self.rpc_monitor.stop()
        start_time = time.time()
        while self.running and time.time() - start_time < 1000:
            time.sleep(0.001)
        if self.running:
            raise Exception("Server did not stop")

    
class PauseSupportMonitor(StateChangeMonitorAPI):

    def __init__(self, pserver):
        self.pserver = pserver

    async def new_state(self, state_map: StateMap,
                        old_state: Union[State, None],
                        new_state: Substate) -> State:
        
        return await self.pserver.new_state(state_map,
                                            old_state,
                                            new_state)

    async def new_substate(self, state_map: StateMap,
                            state: State,
                            substate: Substate) -> None:
        await self.pserver.new_substate(state_map,
                                        state,
                                        substate)

    def finish_state_change(self,  new_state: str) -> None:
        pass

class Interceptor(MessageInterceptor):

    def __init__(self, server):
        self.server = server

    async def before_in_msg(self, message) -> bool:
        return await self.server.before_in_msg(message)

    async def after_in_msg(self, message) -> bool:
        return await self.server.after_in_msg(message)
        
    async def before_out_msg(self, message) -> bool:
        return await self.server.before_out_msg(message)

    async def after_out_msg(self, message) -> bool:
        return await self.server.after_out_msg(message)
    
class PauseReason(str, Enum):

    state = "STATE"
    substate = "SUBSTATE"
    out_before = "OUT_BEFORE"
    out_after = "OUT_AFTER"
    in_before = "IN_BEFORE"
    in_after = "IN_AFTER"
    
@dataclass
class PauseContext:
    server: PServer
    reason: PauseReason
    state: State = None
    substate: Substate = None
    message_code: str = None
