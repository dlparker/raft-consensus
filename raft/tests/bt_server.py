import asyncio
from pathlib import Path
import traceback
import logging
import threading
from logging.config import dictConfig

import raft
from raft.states.memory_log import MemoryLog
from raft.states.log_api import LogRec
from raft.comms.udp import UDPComms
from raft.comms.memory_comms import MemoryComms
from raft.tests.timer import ControlledTimer

class UDPBankTellerServer:

    @classmethod
    def make_and_start(cls, port, working_dir, name, others, log_config):
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
            instance = cls(port, working_dir, name, others)
            instance.start()
        except Exception as e:
            traceback.print_exc()
            raise

    def __init__(self, port, working_dir, name, others):
        self._host = "localhost"
        self._port = port
        self._name = name
        self._working_dir = working_dir
        self._others = others
        
    async def _run(self):
        logger = logging.getLogger(__name__)
        logger.info("bank teller server starting")
        state = raft.state_follower(vote_at_start=True)
        data_log = MemoryLog()
        loop = asyncio.get_running_loop()
        logger.info('creating server')
        server = raft.create_server(name='raft', state=state,
                                    log=data_log, other_nodes=self._others,
                                    endpoint=(self._host, self._port),
                                    comms=UDPComms())
        logger.info(f"{self._name} started server on endpoint {(self._host, self._port)} with others at {self._others}")

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

    def __init__(self, port, working_dir, name, others, log_config=None):
        # log_config is ignored, kept just to match process launching
        # versions to make control code cleaner
        self._host = "localhost"
        self._port = port
        self._name = name
        self._working_dir = working_dir
        self._others = others
        self.thread = ServerThread(port, others)
        self.thread.name = f"{self._port}"

    def start(self):
        self.thread.start()

    def stop(self):
        self.thread.keep_running = False
        

class ServerThread(threading.Thread):

    def __init__(self, port, other_nodes):
        threading.Thread.__init__(self)
        self.host = "localhost"
        self.port = port
        self.other_nodes = other_nodes
        self._server = None
        self.keep_running = True

    def run(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())
        
    async def _run(self):
        logger = logging.getLogger(__name__)
        logger.info("memory comms bank teller server starting")
        state = raft.state_follower(vote_at_start=True)
        data_log = MemoryLog()
        logger.info('creating server')
        comms = MemoryComms(timer_class=ControlledTimer)
        self.server = raft.create_server(name='raft', state=state,
                                         log=data_log, other_nodes=self.other_nodes,
                                         endpoint=(self.host, self.port),
                                         comms=comms)
        logger.info(f"started server on memory addr {(self.host, self.port)} with others at {self.other_nodes}")
        while self.keep_running:
            await asyncio.sleep(0.01)
        await comms.stop()
        self.server.stop()

        
