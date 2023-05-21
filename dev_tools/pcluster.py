#!/usr/bin/env python
import os
import sys
import time
import asyncio
import logging
from pathlib import Path
from raftframe.states.base_state import State, Substate
from dev_tools.pserver import PServer
from dev_tools.log_control import one_proc_log_setup


class PausingCluster:

    def __init__(self, server_count, logging_type=None, base_port=5000,
                 working_dir=None, timeout_basis=0.2):
        self.server_count = server_count
        self.logging_type = logging_type
        self.base_port = base_port
        self.logger = None
        self.timeout_basis = timeout_basis
        if working_dir is None:
            working_dir = Path(f"/tmp/raft_tests")
        self.working_dir = working_dir
        if self.logging_type == "devel_one_proc":
            self.log_config = one_proc_log_setup()
        else:
            logging.getLogger().handlers = []
            self.log_config = None
        self.logger = logging.getLogger(__name__)
        self.nodes = []
        self.servers = []
        for i in range(self.server_count):
            self.nodes.append(('localhost', self.base_port+i))
    
        for i in range(self.server_count):
            this_node = self.nodes[i]
            others = []
            for ot in self.nodes:
                if ot[1] == this_node[1]:
                    continue
                others.append(ot)
            server = PServer(port=this_node[1], working_dir=self.working_dir,
                             name=f"server_{i}", others=others,
                             log_config=self.log_config,
                             timeout_basis=self.timeout_basis)
            self.servers.append(server)

    def start_all(self):
        for server in self.servers:
            server.start()

    def stop_all(self):
        for server in self.servers:
            server.stop()

