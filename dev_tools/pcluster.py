#!/usr/bin/env python
import os
import sys
import time
import asyncio
import pdb
from pathlib import Path
from logging.config import dictConfig
import shutil
import logging
from raftframe.states.base_state import State, Substate
from dev_tools.pserver import PServer
from dev_tools.log_control import config_logging


class PausingCluster:

    def __init__(self, server_count):
        self.server_count = server_count
        self.debugging = False
        self.nodes = []
        self.servers = []
        wdir = Path(f"/tmp/raft_tests/p_server")
        if wdir.exists():
            shutil.rmtree(wdir)
        wdir.mkdir(parents=True)
        config,_ = config_logging(f"{wdir.as_posix()}/server.log")
        dictConfig(config)
        for i in range(self.server_count):
            self.nodes.append(('localhost', 5000+i))
    
        for i in range(self.server_count):
            this_node = self.nodes[i]
            others = []
            for ot in self.nodes:
                if ot[1] == this_node[1]:
                    continue
                others.append(ot)
            server = PServer(this_node[1], wdir,
                         f"server_{i}", others, False)
            self.servers.append(server)

    def start_all(self, pause_after_election=False):
        for server in self.servers:
            server.start()

