import asyncio
import shutil
from pathlib import Path
from multiprocessing import Process

import raft

from log_control import servers_as_procs_log_setup, stop_logging_server 
from bt_server import UDPBankTellerServer


class Cluster:

    def __init__(self, server_count, use_processes=True,
                 logging_type=None, base_port=5000):
        self.base_dir = Path("/tmp/raft_tests")
        self.server_count = server_count
        self.use_procs = use_processes
        self.logging_type = logging_type
        self.base_port = base_port
        self.server_recs = {}
        self.dirs_ready = False
        self.setup_dirs()
        if self.logging_type == "devel":
            self.log_config = servers_as_procs_log_setup()
        else:
            self.log_config = None
        
    def setup_dirs(self):
        if self.base_dir.exists():
            shutil.rmtree(self.base_dir)
        self.base_dir.mkdir()
        wdirs = {}
        for i in range(self.server_count):
            name = f"server_{i}"
            wdir = Path(self.base_dir, name)
            wdirs[name] = wdir
            if wdir.exists():
                shutil.rmtree(wdir)
            wdir.mkdir()
            port = self.base_port + i
            self.server_recs[name] = dict(name=name, working_dir=wdir,
                                           addr=('localhost', port), 
                                           port=port)
        self.dirs_ready = True

    def start_all_servers(self):
        if not self.dirs_ready:
            raise Exception('target dirs not clean, are servers running?')
        all_servers = [ ("localhost", sdef['port']) for sdef in self.server_recs.values() ]
        args_set = []
        for name, srec in self.server_recs.items():
            if self.use_procs and srec.get('proc'):
                raise Exception(f"server {name} process already running")
            elif srec.get('task'):
                raise Exception(f"server {name} task already running")
            others = []
            for addr in all_servers:
                if addr[1] != srec['port']:
                    others.append(addr)
            srec['run_args']= [srec['port'], srec['working_dir'],
                             srec['name'], others, self.log_config]
        for name, srec in self.server_recs.items():
            if self.use_procs:
                s_process = Process(target=UDPBankTellerServer.make_and_start,
                                    args=srec['run_args'])
                s_process.daemon = True
                s_process.start()
                srec['proc'] = s_process
            else:
                raise Exception('not done yet')
        return 

    def get_server_by_addr(self, addr):
        for name, srec in self.server_recs.items():
            if srec[addr] == addr:
                return srec
        return None
        
    def start_one_server(self, name):
        if self.use_procs and srec.get('proc'):
            raise Exception(f"server {name} process already running")
        srec = self.server_recs[name]
        s_process = Process(target=UDPBankTellerServer.make_and_start,
                            args=srec['run_args'])
        s_process.daemon = True
        s_process.start()
        srec['proc'] = s_process
        
    def stop_server(self, name):
        s_process = self.server_recs[name].get('proc', None)
        if self.use_procs and s_process:
            s_process.terminate()
            s_process.join()
            del self.server_recs[name]['proc']

    def stop_all_servers(self):
        for name, srec in self.server_recs.items():
            if self.use_procs:
                s_process = srec.get('proc', None)
                if s_process:
                    s_process.terminate()
                    s_process.join()
                    del srec['proc']

    def stop_logging_server(self):
        if self.logging_type is None:
            return
        stop_logging_server()
