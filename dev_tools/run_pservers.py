#!/usr/bin/env python
import os
import sys
from pathlib import Path
from logging.config import dictConfig
import shutil
import logging

if __name__=="__main__":
    #sdir = Path(__file__).parent.resolve()
    #raft_dir = sdir.parent
    #basedir = raft_dir.parent
    #sys.path.append(basedir.as_posix())
    
    from dev_tools.pserver import PServer
    from dev_tools.log_control import config_logging
    if len(sys.argv) < 2:
        raise Exception('pick a number of servers to run, from 2 to X')
    scount = int(sys.argv[1])
    nodes = []
    for i in range(scount):
        nodes.append(('localhost', 5000+i))

    
    servers = []
    for i in range(scount):
        this_node = nodes[i]
        others = []
        for ot in nodes:
            if ot[1] == this_node[1]:
                continue
            others.append(ot)

        wdir = Path(f"/tmp/raft_tests/server_{i}")
        if wdir.exists():
            shutil.rmtree(wdir)
        wdir.mkdir(parents=True)
        os.chdir(wdir)
        config,_ = config_logging(f"{wdir.as_posix()}/server.log")
        dictConfig(config)
        server = PServer(this_node[1], wdir,
                         f"server_{i}", others, False)
        servers.append(server)

    for server in servers:
        server.start()
