import shutil
from pathlib import Path
from multiprocessing import Process
import raft

from bt_server import UDPBankTellerServer

def setup_test_dirs(num_servers):
    base_dir = Path("/tmp/raft_tests")
    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir()
    sdirs = {}
    for i in range(num_servers):
        name = f"server_{i}"
        sdir = Path(base_dir, name)
        sdirs[name] = sdir
        sdir.mkdir()
    return dict(base_dir=base_dir, server_dirs=sdirs)

def start_servers(base_port, num_servers=3, reset_dirs=True, log_config=None):
    test_dirs = setup_test_dirs(num_servers)
    all_servers = [ ("localhost", base_port + port) for port in range(num_servers) ]
    servers = {}
    port_index = 0
    for sname,sdir in test_dirs['server_dirs'].items():
        others = []
        for i in range(num_servers):
            if i == port_index:
                continue
            others.append(all_servers[i])
        s_process = Process(target=UDPBankTellerServer.make_and_start,
                                 args=[base_port + port_index, sdir,
                                       sname, others, log_config])
        s_process.daemon = True
        s_process.start()
        servers[sname] = dict(port=base_port + port_index,
                              process=s_process, name=sname, working_dir=sdir)
        port_index += 1
    return servers

def start_one_server(name, working_dir):
    s_process = Process(target=BankTeller.make_and_start,
                        args=[working_dir, name])
    result = dict(process=s_process, name=name, working_dir=working_dir)
    return result

def stop_server(server_def):
    s_process = server_def['process']
    s_process.terminate()
    s_process.join()
    
