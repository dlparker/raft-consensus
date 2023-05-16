#

import asyncio
from pathlib import Path
import traceback
import logging
import threading
import time
import multiprocessing
from logging.config import dictConfig

import raftframe
from raftframe.servers.server_config import LiveConfig, ClusterConfig, LocalConfig
from raftframe.servers.server import Server
from raftframe.comms.udp import UDPComms
from raftframe.states.state_map import StandardStateMap
from raftframe.app_api.app import StateChangeMonitor
from raftframe.states.follower import Follower
from raftframe.serializers.msgpack import MsgpackSerializer
from raftframe.serializers.json import JsonSerializer
from bank_teller.bank_app import BankingApp
from dev_tools.timer_wrapper import ControlledTimer, get_timer_set
from dev_tools.memory_log import MemoryLog
from dev_tools.memory_comms import MemoryComms, MessageInterceptor

manager = multiprocessing.Manager()
csns = manager.Namespace()
csns.status = manager.dict()
csns.control = manager.dict()

def get_bt_server_cs_dict(name):
    return csns.status.get(name, None)

def clear_bt_server_cs_dict(name):
    csns.status[name] = manager.dict()
    
class MPStandardStateMap(StandardStateMap):

    def __init__(self, *args, **kwargs):
        self.bt_server_name = kwargs.pop('bt_server_name')
        csns.status[self.bt_server_name] = manager.dict()
        super().__init__(*args, **kwargs)
        
    async def set_substate(self, state, substate):
        await super().set_substate(state, substate)
        csns.status[self.bt_server_name]['state_type'] = str(state)
        csns.status[self.bt_server_name]['substate'] = substate
    
    
class UDPBankTellerServer:

    @classmethod
    def make_and_start(cls, port, working_dir, name, others,
                       log_config, timeout_basis=0.1):
        from pytest_cov.embed import cleanup_on_sigterm
        cleanup_on_sigterm()
        import sys
        output_path = Path(working_dir, "server.out")
        sys.stdout = open(output_path, 'w')
        sys.stderr = sys.stdout
        import os
        os.chdir(working_dir)
        logging.getLogger().handlers = []
        if log_config:
            from pprint import pprint
            try:
                dictConfig(log_config)
                #pprint(log_config)
            except:
                pprint(log_config)
                raise
        try:
            instance = cls(port, working_dir, name, others,
                           timeout_basis)
            instance.start()
        except Exception as e:
            traceback.print_exc()
            raise
        return instance

    def __init__(self, port, working_dir, name, others,
                 timeout_basis):
        self.host = "localhost"
        self.port = port
        self.name = name
        self.working_dir = working_dir
        self.others = others
        self.timeout_basis = timeout_basis
        self.running = False
        
    async def _run(self):
        try:
            logger = logging.getLogger(__name__)
            logger.info("bank teller server starting")
            state_map = MPStandardStateMap(bt_server_name=self.name,
                                           timeout_basis=self.timeout_basis)
            data_log = MemoryLog()
            loop = asyncio.get_running_loop()
            logger.info('creating server')
            endpoint = (self.host, self.port)
            app = BankingApp()
            cc = ClusterConfig(name=f"{endpoint}",
                          endpoint=endpoint,
                          other_nodes=self.others)
            local_config = LocalConfig(working_dir=self.working_dir)
            self.live_config = LiveConfig(cluster=cc,
                                          local=local_config,
                                          app=app, log=data_log,
                                          comms=UDPComms(),
                                          state_map=state_map,
                                          serializer=MsgpackSerializer())
            server = Server(live_config=self.live_config)
            server.start()
            logger.info(f"{self.name} started server on endpoint {(self.host, self.port)} with others at {self.others}")
        except :
            logger.error(traceback.format_exc())

    def start(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())
        try:
            # run_forever() returns after calling loop.stop()
            loop.run_forever()
            tasks = asyncio.all_tasks()
            for t in [t for t in tasks if not (t.done() or t.cancelled())]:
                # give canceled tasks the last chance to run
                loop.run_until_complete(t)
        finally:
            loop.close()        
            self.running = False

        
        
class MemoryBankTellerServer:

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
        self.state_map = StandardStateMap(timeout_basis=timeout_basis)
        self.data_log = MemoryLog()
        self.comms = MemoryComms()
        self.app = BankingApp()
        self.thread = ServerThread(self)
        self.thread_started = False
        self.thread.name = f"{self.port}"
        self.thread_ident = None

    def add_other_server(self, other):
        self.thread.add_other_server(other)
        
    async def pause_on_reason(self, reason_string, propogate=True):
        await self.thread.pause_on_reason(reason_string, propogate)

    async def resume_from_reason(self, reason_string, propogate=True):
        await self.thread.resume_from_reason(reason_string, propogate)
        
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

    async def in_loop_check(self, thread_obj):
        # override this to do something from inside the thread
        # main loop
        pass
        
class ServerThread(threading.Thread):

    def __init__(self, bt_server):
        threading.Thread.__init__(self)
        self.bt_server = bt_server
        self.host = "localhost"
        self.ready = False
        self.keep_running = True
        self.running = False
        self.server = None
        self.other_servers = []
        self.logger = logging.getLogger(__name__)

    def add_other_server(self, other):
        # this is so the pause operation can stop all servers
        self.other_servers.append(other)

    async def pause_on_reason(self, reason_string, propogate=True):
        await get_timer_set().pause_all()
        if propogate:
            for other in self.other_servers:
                other.pause_on_reason(reason_string, False)

    async def resume_from_reason(self, reason_string, propogate=True):
        await get_timer_set().resume()
        if propogate:
            for other in self.other_servers:
                other.resume_from_reason(reason_string, False)

    def run(self):
        self.bt_server.thread_ident = threading.get_ident()
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

    def go(self):
        self.ready = True
        
    def configure(self, serializer_class=MsgpackSerializer):
        if self.server:
            return
        
        self.logger.info('creating server')
        cc = ClusterConfig(name=self.bt_server.name,
                           endpoint=self.bt_server.endpoint,
                           other_nodes=self.bt_server.other_nodes)
        local_config = LocalConfig(working_dir=self.bt_server.working_dir)
        self.live_config = LiveConfig(cluster=cc,
                                      local=local_config,
                                      app=self.bt_server.app,
                                      log=self.bt_server.data_log,
                                      comms=self.bt_server.comms,
                                      state_map=self.bt_server.state_map,
                                      serializer=serializer_class())
        self.server = Server(live_config=self.live_config)
        self.server.set_timer_class(ControlledTimer)
        self.server.get_endpoint()
        
    async def _run(self):
        self.running = True
        try:
            self.server.start()
            self.logger.info("started server on memory addr %s  with others at %s",
                        self.bt_server.endpoint,
                        self.bt_server.other_nodes)
            while self.keep_running:
                await asyncio.sleep(0.01)
                await self.bt_server.in_loop_check(self)
            self.logger.info("server %s stopping", self.bt_server.endpoint)
            await self.bt_server.comms.stop()
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



        
        
