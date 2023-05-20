#!/usr/bin/env python
import os
import sys
import time
import asyncio
import logging
from pathlib import Path
from raftframe.states.base_state import State, Substate
from dev_tools.pserver import PServer


class PausingCluster:

    def __init__(self, server_count, working_dir=None):
        self.server_count = server_count
        self.debugging = False
        if working_dir is None:
            working_dir = Path(f"/tmp/raft_tests")
        self.working_dir = working_dir
        self.nodes = []
        self.servers = []
        for i in range(self.server_count):
            self.nodes.append(('localhost', 5000+i))
    
        for i in range(self.server_count):
            this_node = self.nodes[i]
            others = []
            for ot in self.nodes:
                if ot[1] == this_node[1]:
                    continue
                others.append(ot)
            server = PServer(this_node[1], self.working_dir,
                         f"server_{i}", others, False)
            self.servers.append(server)

    def start_all(self, pause_after_election=False):
        for server in self.servers:
            server.start()

