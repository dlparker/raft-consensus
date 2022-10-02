import asyncio
import shutil
from pathlib import Path
from multiprocessing import Process
import logging
import raft

from log_control import servers_as_procs_log_setup, stop_logging_server 
from log_control import one_proc_log_setup
from bt_server import UDPBankTellerServer, MemoryBankTellerServer


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
        #logging_type = "devel_one_proc" when using Mem comms and thread based servers
        #logging_type = "devel_mp" when using UDP comms and MP process based servers
        #logging_type = "silent" for no log at all
        if self.logging_type == "devel_mp":
            self.log_config = servers_as_procs_log_setup()
        elif self.logging_type == "devel_one_proc":
            self.log_config = one_proc_log_setup()
        elif self.logging_type == "silent":
            logging.getLogger().handlers = []
            self.log_config = None
        else:
            raise Exception(f"invalid logging type {logging_type}")
        
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
            elif srec.get("tserver"):
                raise Exception(f"server {name} tserver already running")
            others = []
            for addr in all_servers:
                if addr[1] != srec['port']:
                    others.append(addr)
            srec['run_args']= [srec['port'], srec['working_dir'],
                               srec['name'], others, self.log_config,
                               False]
        for name, srec in self.server_recs.items():
            self.start_one_server(name)
        return 

    def get_server_by_addr(self, addr):
        for name, srec in self.server_recs.items():
            if srec[addr] == addr:
                return srec
        return None
        
    def start_one_server(self, name, vote_at_start=True):
        # vote_at_start True means that server starts with
        # a follower that does not wait for timeout, which
        # makes testing go faster. Sometimes you want the
        # timeout to happen, so set to False
        srec = self.server_recs[name]
        # vote at start is last arg
        args = [ item for item in srec['run_args'][:-1] ]
        args.append(vote_at_start)
        if self.use_procs:
            if srec.get('proc'):
                raise Exception(f"server {name} process already running")
            srec = self.server_recs[name]
            s_process = Process(target=UDPBankTellerServer.make_and_start,
                                args=args)
            s_process.daemon = True
            s_process.start()
            srec['proc'] = s_process
        else:
            if srec.get('tserver'):
                raise Exception(f"server {name} tserver already running")
            tserver = MemoryBankTellerServer(*args)
            tserver.start()
            srec['tserver'] = tserver
            
    def stop_server(self, name):
        srec = self.server_recs[name]
        if self.use_procs:
            s_process = srec.get('proc', None)
            if s_process:
                s_process.terminate()
                s_process.join()
                del srec['proc']
        else:
            tserver = srec.get('tserver', None)
            if tserver:
                tserver.stop()
                del srec['tserver']

    def stop_all_servers(self):
        for name, srec in self.server_recs.items():
            self.stop_server(name)

    def stop_logging_server(self):
        if self.logging_type is None:
            return
        stop_logging_server()
