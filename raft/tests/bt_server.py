# TODO: Fix name, it is messed up

import asyncio
from pathlib import Path
import traceback
import logging
import threading
from logging.config import dictConfig

import raft
from raft.servers.server import Server
from raft.log.memory_log import MemoryLog
from raft.comms.udp import UDPComms
from raft.comms.memory_comms import MemoryComms
from raft.states.state_map import StandardStateMap
from raft.states.follower import Follower
from bank_teller.bank_app import BankingApp

from raft.tests.timer import ControlledTimer
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
        
    async def _run(self):
        try:
            logger = logging.getLogger(__name__)
            logger.info("bank teller server starting")
            state_map = StandardStateMapWrapper()
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
            logger.info(f"{self.name} started server on endpoint {(self.host, self.port)} with others at {self.others}")
        except:
            logger.error(traceback.print_exc())

    def start(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())
        loop.run_forever()
        loop.stop()


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
        self.comms = MemoryComms(timer_class=ControlledTimer)
        self.app = BankingApp()
        self.thread = ServerThread(self)
        self.thread.name = f"{self.port}"

    def start(self):
        self.thread.start()
        return self.thread
        
    def stop(self):
        self.thread.stop()
        self.thread.keep_running = False

class ServerThread(threading.Thread):

    def __init__(self, bt_server):
        threading.Thread.__init__(self)
        self.bt_server = bt_server
        self.host = "localhost"
        self.keep_running = True

    def run(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())
        
    async def _run(self):
        try:
            logger = logging.getLogger(__name__)
            logger.info("memory comms bank teller server starting")
            logger.info('creating server')
            self.server = Server(name=self.bt_server.name,
                                 state_map=self.bt_server.state_map,
                                 log=self.bt_server.data_log,
                                 other_nodes=self.bt_server.other_nodes,
                                 endpoint=self.bt_server.endpoint,
                                 comms=self.bt_server.comms,
                                 app=self.bt_server.app)
            logger.info("started server on memory addr %s  with others at %s",
                        self.bt_server.endpoint,
                        self.bt_server.other_nodes)
            self.server.get_endpoint()
            while self.keep_running:
                await asyncio.sleep(0.01)
            await self.bt_server.comms.stop()
        except:
            print("\n\n!!!!!!Server thread failed!!!!")
            traceback.print_exc()
        
    def stop(self):
        self.server.stop()
        self.keep_running = False
