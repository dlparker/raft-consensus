import asyncio
from pathlib import Path
import traceback
import logging
import raft

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
        from logging.config import dictConfig
        from pprint import pprint
        try:
            dictConfig(log_config)
            pprint(log_config)
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
        data_log = []
        dummy_index = {
            'term': None,
            'command': None,
            'balance': None
        }
        data_log.append(dummy_index)
        loop = asyncio.get_running_loop()
        logger.info('creating server')
        server = raft.create_server(name='raft', state=state,
                                    log=data_log, other_nodes=self._others,
                                    endpoint=(self._host, self._port), loop=loop)
        logger.info('created server')
        print(f"{self._name} started server on endpoint {(self._host, self._port)} with others at {self._others}", flush=True)

    def start(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())
        loop.run_forever()
        loop.stop()
