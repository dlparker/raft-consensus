# TODO: Fix name, it is messed up

import asyncio
from pathlib import Path
import traceback
import logging
import threading
import time
from logging.config import dictConfig

import raft
from raft.servers.server import Server
from raft.log.memory_log import MemoryLog
from raft.comms.udp import UDPComms
from raft.comms.memory_comms import MemoryComms
from raft.states.state_map import StandardStateMap
from raft.states.follower import Follower
from bank_teller.bank_app import BankingApp

from raft.tests.timer import ControlledTimer, get_timer_set
from raft.tests.wrappers import FollowerWrapper
from raft.tests.wrappers import StandardStateMapWrapper

    
class UDPBankTellerServer:

    @classmethod
    def make_and_start(cls, port, working_dir, name, others,
                       log_config, vote_at_start=True):
        # vote_at_start True means that server starts with
        # a follower that does not wait for timeout, which
        # makes testing go faster. Sometimes you want the
        # timeout to happen, so set to False
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
            instance = cls(port, working_dir, name, others, vote_at_start)
            instance.start()
        except Exception as e:
            traceback.print_exc()
            raise

    def __init__(self, port, working_dir, name, others, vote_at_start):
        # vote_at_start True means that server starts with
        # a follower that does not wait for timeout, which
        # makes testing go faster. Sometimes you want the
        # timeout to happen, so set to False
        self.host = "localhost"
        self.port = port
        self.name = name
        self.working_dir = working_dir
        self.others = others
        self.vote_at_start = vote_at_start
        self.running = False
        
    async def _run(self):
        try:
            logger = logging.getLogger(__name__)
            logger.info("bank teller server starting")
            state_map = StandardStateMap()
            data_log = MemoryLog()
            loop = asyncio.get_running_loop()
            logger.info('creating server')
            endpoint = (self.host, self.port)
            app = BankingApp()
            server = Server(name=f"{endpoint}", state_map=state_map,
                            log=data_log, other_nodes=self.others,
                            endpoint=endpoint,
                            comms=UDPComms(),
                            app=app)
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
                 log_config=None, vote_at_start=True):
        # vote_at_start True means that server starts with
        # a follower that does not wait for timeout, which
        # makes testing go faster. Sometimes you want the
        # timeout to happen, so set to False
        # log_config is ignored, kept just to match process launching
        # versions to make control code cleaner
        self.host = "localhost"
        self.port = port
        self.working_dir = working_dir
        self.other_nodes = others
        self.endpoint = (self.host, self.port)
        self.name = f"{self.endpoint}"
        self.state_map = StandardStateMapWrapper()
        self.data_log = MemoryLog()
        self.comms = MemoryComms()
        self.app = BankingApp()
        self.thread = ServerThread(self)
        self.thread_started = False
        self.thread.name = f"{self.port}"

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
        self.bt_server.comms.pause()
        await get_timer_set().pause_all_this_thread()
        if propogate:
            for other in self.other_servers:
                other.pause_on_reason(reason_string, False)

    async def resume_from_reason(self, reason_string, propogate=True):
        self.bt_server.comms.resume()
        await get_timer_set().resume_all_this_thread()
        if propogate:
            for other in self.other_servers:
                other.resume_from_reason(reason_string, False)

    def run(self):
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
        
    def configure(self):
        if self.server:
            return
        self.logger.info('creating server')
        if hasattr(self.bt_server.state_map, "set_server_wrapper"):
            self.bt_server.state_map.set_server_wrapper(self)
        self.server = Server(name=self.bt_server.name,
                             state_map=self.bt_server.state_map,
                             log=self.bt_server.data_log,
                             other_nodes=self.bt_server.other_nodes,
                             endpoint=self.bt_server.endpoint,
                             comms=self.bt_server.comms,
                             app=self.bt_server.app)
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
            await self.bt_server.comms.stop()
            await self.server.stop()
        except:
            print("\n\n!!!!!!Server thread failed!!!!")
            traceback.print_exc()
        self.running = False

    def stop(self):
        self.keep_running = False
        start_time = time.time()
        while self.running and time.time() - start_time < 1:
            time.sleep(0.001)
        if self.running:
            raise Exception("Server did not stop")
