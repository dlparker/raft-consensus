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
from raft.tests.timer import ControlledTimer
from bank_teller.bank_app import BankingApp

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
            state = raft.state_follower(vote_at_start=self.vote_at_start)
            data_log = MemoryLog()
            loop = asyncio.get_running_loop()
            logger.info('creating server')
            endpoint = (self.host, self.port)
            app = BankingApp()
            server = Server(name=f"{endpoint}", state=state,
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
        self.name = name
        self.working_dir = working_dir
        self.others = others
        self.thread = ServerThread(port, others, vote_at_start)
        self.thread.name = f"{self.port}"

    def start(self):
        self.thread.start()
        
    def stop(self):
        self.thread.stop()
        self.thread.keep_running = False

class ServerThread(threading.Thread):

    def __init__(self, port, other_nodes, vote_at_start):
        # vote_at_start True means that server starts with
        # a follower that does not wait for timeout, which
        # makes testing go faster. Sometimes you want the
        # timeout to happen, so set to False
        threading.Thread.__init__(self)
        self.host = "localhost"
        self.port = port
        self.other_nodes = other_nodes
        self.keep_running = True
        self.vote_at_start = vote_at_start

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
            state = raft.state_follower(vote_at_start=self.vote_at_start)
            data_log = MemoryLog()
            logger.info('creating server')
            comms = MemoryComms(timer_class=ControlledTimer)
            endpoint = (self.host, self.port)
            app = BankingApp()
            self.server = Server(name=f"{endpoint}", state=state,
                            log=data_log, other_nodes=self.other_nodes,
                            endpoint=endpoint,
                            comms=comms,
                            app=app)
            logger.info(f"started server on memory addr {(self.host, self.port)} with others at {self.other_nodes}")
            self.server.get_endpoint()
            while self.keep_running:
                await asyncio.sleep(0.01)
            await comms.stop()
        except:
            print("\n\n!!!!!!Server thread failed!!!!")
            traceback.print_exc()
        
    def stop(self):
        self.server.stop()
        self.keep_running = False
