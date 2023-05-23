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
            server = self.prepare_server(this_node)
            self.servers.append(server)

    def prepare_server(self, addr):
        others = []
        for ot in self.nodes:
            if ot == addr:
                continue
            others.append(ot)
        server = PServer(port=addr[1], working_dir=self.working_dir,
                         name=f"server_{addr[1]}", others=others,
                         log_config=self.log_config,
                         timeout_basis=self.timeout_basis)
        return server
        
    def start_all(self):
        for server in self.servers:
            server.start()

    def pause_all(self):
        for server in self.servers:
            server.direct_pause()

    def resume_all(self):
        for server in self.servers:
            server.resume()
            
    def stop_all(self):
        for server in self.servers:
            server.stop()

    def regen_server(self, stopped_server):
        index = 0
        for server in self.servers:
            if stopped_server == server:
                server = self.prepare_server(server.endpoint)
                self.servers[index] = server
                return server
            index += 1
