from __future__ import annotations
import asyncio
import time
import shutil
from pathlib import Path
from multiprocessing import Process
import logging
import dataclasses
from dataclasses import dataclass, field
from typing import Tuple, List, Union
from enum import Enum
import abc

from raft.dev_tools.bt_server import UDPBankTellerServer
from raft.dev_tools.bt_server import clear_bt_server_cs_dict, get_bt_server_cs_dict
from raft.dev_tools.bt_client import UDPBankTellerClient
from raft.dev_tools.log_control import (servers_as_procs_log_setup,
                                        stop_logging_server,
                                        have_logging_server)

@dataclass
class ServerSpec:
    name: str
    port: int
    addr: Tuple[str, int] = field(repr=False)
    working_dir: Path = field(repr=False)
    run_args: List = field(repr=False)
    process: Process = field(repr=False, default=None)
    running: bool = field(repr=False, default=False)
    client: UDPBankTellerClient = field(repr=False, default=None)
    role: str  = field(repr=False, default=None)
    
    def get_client(self, force_new=False):
        if not self.client or force_new:
            self.client = UDPBankTellerClient(*self.addr)
        return self.client
        
class UDPServerCluster:

    def __init__(self, server_count, logging_type=None, base_port=5000):
        self.base_dir = Path("/tmp/raft_tests")
        self.server_count = server_count
        self.logging_type = logging_type
        self.base_port = base_port
        self.server_specs = {}
        self.dir_recs = {}
        self.all_server_addrs = []
        if self.logging_type == "devel_mp":
            self.log_config = servers_as_procs_log_setup()
        else:
            logging.getLogger().handlers = []
            self.log_config = None
        self.logger = logging.getLogger(__name__)

    def get_servers(self):
        return self.server_specs

    def prepare_one(self, name, restart=False, timeout_basis=1.0):
        if restart:
            self.setup_server_dir(name)
        others = []
        dir_rec = self.dir_recs[name]
        for addr in self.all_server_addrs:
            if addr[1] != dir_rec['port']:
                others.append(addr)
        args = [dir_rec['port'], dir_rec['working_dir'],
                dir_rec['name'], others, self.log_config,
                timeout_basis]
        
        spec = ServerSpec(dir_rec['name'], dir_rec['port'],
                          dir_rec['addr'], dir_rec['working_dir'],
                          run_args=args,
                          process=None)
        clear_bt_server_cs_dict(spec.name)
        self.server_specs[spec.name] = spec
        return spec
        
    def prepare(self, timeout_basis=1.0):
        if len(self.server_specs) > 0:
            raise Exception("cannot call prepare more than once")
        self.dir_recs = self.setup_dirs()
        self.all_server_addrs = [ sdef['addr'] for sdef in 
                                  self.dir_recs.values() ]
        for name, dir_rec in self.dir_recs.items():
            self.prepare_one(name, timeout_basis=timeout_basis)
        return self.server_specs

    def start_all_servers(self):
        for spec in self.server_specs.values():
            if spec.running:
                continue
            self.start_one_server(spec.name)

    def start_one_server(self, name):
        if self.logging_type == "devel_mp":
            # doesn't seem to work to import in module global space
            from raft.dev_tools.log_control import have_logging_server
            if not have_logging_server:
                raise Exception('no logging server!')
                
        spec = self.server_specs[name]
        if spec.running:
            raise Exception("cannot start already running server")
        process = Process(target=UDPBankTellerServer.make_and_start,
                          args=spec.run_args)
        process.daemon = True
        process.start()
        spec.process = process
        spec.running = True

    def setup_server_dir(self, name):
        wdir = Path(self.base_dir, name)
        if wdir.exists():
            shutil.rmtree(wdir)
        wdir.mkdir()
        return wdir
    
    def setup_dirs(self):
        if not self.base_dir.exists():
            self.base_dir.mkdir()
        result = {}
        for i in range(self.server_count):
            name = f"server_{i}"
            port = self.base_port + i
            addr = ('localhost', port)
            wdir = self.setup_server_dir(name)
            result[name] = dict(name=name, working_dir=wdir,
                                port=port, addr=addr)
        return result

    def get_server_by_addr(self, addr):
        for name, spec in self.server_specs.items():
            if spec.addr == addr:
                return spec
        return None

    def wait_for_state(self, state_type="any", server_name=None, timeout=3):
        expected = []
        if server_name is None:
            for sname,spec in self.server_specs.items():
                if not spec.running:
                    continue
                expected.append(sname)
        else:
            spec = self.server_specs[server_name]
            if not spec.running:
                raise Exception(f"cannot wait for server {server_name}," \
                                " not running")
            expected.append(server_name)

        start_time = time.time()
        while time.time() - start_time < timeout:
            found = []
            for sname in expected:
                if sname in found:
                    continue
                sstat = get_bt_server_cs_dict(sname)
                if sstat is None:
                    continue
                if sstat.get('state_type') is None:
                    continue
                if state_type == "any":
                    found.append(sname)
                else:
                    if sstat['state_type'] == state_type:
                        found.append(sname)
            if len(found) == len(expected):
                break
        
        if len(found) != len(expected):
            raise Exception(f"timeout waiting for {state_type}, " \
                            f"expected '{expected}', got '{found}'")
        
            
    def stop_server(self, name):
        spec = self.server_specs[name]
        if not spec.running:
            return
        spec.process.terminate()
        spec.process.join()
        spec.process = None
        spec.running = False

    def stop_all_servers(self):
        for name in self.server_specs.keys():
            self.stop_server(name)
        stop_logging_server()
        
    def stop_logging_server(self):
        stop_logging_server()
