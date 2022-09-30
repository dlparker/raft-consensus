import os
import sys
from pathlib import Path
from logging.config import dictConfig
import shutil
import logging
sdir = Path(__file__).parent.resolve()
raft_dir = sdir.parent
basedir = raft_dir.parent
sys.path.append(basedir.as_posix())

from bt_server import UDPBankTellerServer
from log_control import config_logging

nodes = [('localhost', 5000), ('localhost', 5001), ('localhost', 5002)]
if len(sys.argv) < 2:
    raise Exception('pick a port from 0,1,2 (5000,5001,5002)')
index = int(sys.argv[1])
this_node = nodes[index]
others = []
for ot in nodes:
    if ot[1] == this_node[1]:
        continue
    others.append(ot)


wdir = Path(f"/tmp/raft_tests/server_{index}")
if wdir.exists:
    shutil.rmtree(wdir)
wdir.mkdir()
os.chdir(wdir)
config,_ = config_logging(f"{wdir.as_posix()}/server.log")
dictConfig(config)
server = UDPBankTellerServer(this_node[1],wdir,
                             f"server_{index}", others)
        
server.start()
