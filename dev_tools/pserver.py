#

import asyncio
from pathlib import Path
import traceback
import logging
import threading
import time
import multiprocessing
from typing import Union
from logging.config import dictConfig

import raftframe
from raftframe.servers.server_config import LiveConfig, ClusterConfig, LocalConfig
from raftframe.servers.server import Server
from raftframe.states.state_map import StandardStateMap, StateMap
from raftframe.states.base_state import State, Substate
from raftframe.app_api.app import StateChangeMonitor
from raftframe.states.follower import Follower
from raftframe.serializers.msgpack import MsgpackSerializer
from dev_tools.bank_app import BankingApp
from dev_tools.timer_wrapper import ControlledTimer, get_timer_set
from dev_tools.memory_log import MemoryLog
from dev_tools.memory_comms import MemoryComms, MessageInterceptor


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
        self.endpoint = (self.host, self.port)
        self.thread_started = False
        self.thread_ident = None
        self.running = False
        self.paused = False
        self.pause_callback = None
        self.pause_noted = False
        self.resume_callback = None
        self.resume_noted = False
        self.do_log_stats = False
        self.do_dump_state = False
        self.do_resume = False
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}
        self.pausing_substates = {}
        self.pausing_states = {}
        self.logger = logging.getLogger(__name__)
        self.state_map = StandardStateMap(timeout_basis=timeout_basis)
        self.data_log = MemoryLog()
        self.comms = MemoryComms()
        self.app = BankingApp()
        self.thread = ServerThread(self)
        self.thread.name = f"{self.port}"
        self.monitor = PauseSupportMonitor(self)
        self.state_map.add_state_change_monitor(self.monitor)

    async def pause_timers(self):
        await self.thread.pause_timers()

    async def resume_timers(self):
        await self.thread.resume_timers()
        
    async def pause_new_messages(self):
        self.comms.pause_new_messages()

    async def resume_new_messages(self):
        self.comms.resume_new_messages()

    async def pause(self):
        await self.pause_timers()
        await self.pause_new_messages()
        self.paused = True
        self.logger.debug("%s, %s paused", self.name, self.thread_ident)

    def resume(self):
        self.do_resume = True
    
    def pause_on_state(self, state):
        self.pausing_states[str(state)] = self.state_pause_method

    def clear_pause_on_state(self, state: State):
        if str(state) in self.pausing_states:
            del self.pausing_states[str(state)]
         
    async def state_pause_method(self, state_map, old_state, new_state):
        await self.pause()

    async def new_state(self, state_map: StateMap,
                        old_state: Union[State, None],
                        new_state: Substate) -> State:
        if str(new_state) in self.pausing_states:
            self.logger.debug("%s, %s pausing on %s", self.name, self.thread_ident, new_state)
            return await self.pausing_states[str(new_state)](state_map, old_state, new_state)
        return new_state
                
    def pause_on_substate(self, substate):
        self.pausing_substates[str(substate)] = self.substate_pause_method

    def clear_pause_on_substate(self, substate: Substate):
        if str(substate) in self.pausing_substates:
            del self.pausing_substates[str(substate)]

    async def substate_pause_method(self, state_map,
                                    state, substate):
        await self.pause()

    async def new_substate(self, state_map: StateMap,
                            state: State,
                            substate: Substate) -> None:
        if str(substate) in self.pausing_substates:
            self.logger.debug("%s, %s pausing on %s", self.name, self.thread_ident, substate)
            await self.pausing_substates[str(substate)](state_map, state, substate)
            
    async def interceptor_pause_method(self, mode, code, message):
        await self.pause()
        return True
        
    async def pause_before_out_message(self, code):
        self.out_befores[code] = self.intercept_method

    async def pause_after_out_message(self, code):
        self.out_afters[code] = self.intercept_method
        
    async def pause_before_in_message(self, code):
        self.in_befores[code] = self.intercept_method

    async def pause_after_in_message(self, code):
        self.in_afters[code] = self.intercept_method

    def clear_message_triggers(self):
        self.in_befores = {}
        self.in_afters = {}
        self.out_befores = {}
        self.out_afters = {}

    async def before_in_msg(self, message) -> bool:
        if message.code in self.in_befores:
            self.logger.debug("%s, %s pausing before in %s", self.name, self.thread_ident, message.code)
            return await self.interceptor_pause_method('IN_BEFORE', message.code, message)
        return True

    async def after_in_msg(self, message) -> bool:
        if message.code in self.in_afters:
            self.logger.debug("%s, %s pausing after in %s", self.name, self.thread_ident, message.code)
            return await self.interceptor_pause_method('IN_AFTER', message.code, message)
        return True

    async def before_out_msg(self, message) -> bool:
        if message.code in self.out_befores:
            self.logger.debug("%s, %s pausing before out %s", self.name, self.thread_ident, message.code)
            return await self.interceptor_pause_method('OUT_BEFORE', message.code, message)
        return True

    async def after_out_msg(self, message) -> bool:
        if message.code in self.out_afters:
            self.logger.debug("%s, %s pausing after out %s", self.name, self.thread_ident, message.code)
            return await self.interceptor_pause_method('OUT_AFTER', message.code, message)
        return True

    def start_thread(self):
        if not self.thread_started:
            self.thread.start()
            self.thread_started = True
        return self.thread

    def start(self):
        if not self.thread_started:
            self.thread.start()
            self.thread_started = True
        self.thread.go()

    def configure(self):
        self.thread.configure()
        return self.thread
        
    def stop(self):
        self.thread.stop()
        self.thread.keep_running = False
        self.logger.debug("%s, %s stopped", self.name, self.thread_ident)

    async def in_loop_check(self, thread_obj):
        # this is called from the server thread object in that thread

        if self.paused and not self.pause_noted:
            self.pause_noted = True
            if self.pause_callback:
                await self.pause_callback(self)
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
            timer_set = get_timer_set()
            timer_set.resume_all()
            self.do_resume = False
            self.logger.info("<<<<<<< %s %s resumed", self.port,
                             self.state_map.state)
            if not self.resume_noted:
                self.resume_noted = True
                if self.resume_callback:
                    await self.resume_callback(self)
        
class ServerThread(threading.Thread):

    def __init__(self, pserver):
        threading.Thread.__init__(self)
        self.pserver = pserver
        self.host = "localhost"
        self.ready = False
        self.keep_running = True
        self.running = False
        self.server = None
        self.logger = logging.getLogger(__name__)

    async def pause_timers(self):
        await get_timer_set().pause_all()

    async def resume_timers(self):
        await get_timer_set().resume()

    def run(self):
        self.pserver.thread_ident = threading.get_ident()
        self.pserver.running = True
        if not self.ready:
            self.logger.info("memory comms bank teller server waiting for config")
            while not self.ready:
                time.sleep(0.001)
        if self.server is None:
            self.configure()
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
        
    def configure(self, serializer_class=MsgpackSerializer):
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
                                      serializer=serializer_class())
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
        start_time = time.time()
        while self.running and time.time() - start_time < 1000:
            time.sleep(0.001)
        if self.running:
            raise Exception("Server did not stop")



        
        
class PauseSupportMonitor(StateChangeMonitor):

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

