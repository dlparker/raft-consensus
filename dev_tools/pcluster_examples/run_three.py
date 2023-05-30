#!/usr/bin/env python
"""
"""
import os
import time
import time
import asyncio
from pathlib import Path
import shutil
from raftframe.states.base_state import State, Substate
from dev_tools.pcluster import PausingCluster
from dev_tools.log_control import one_proc_log_setup


if __name__=="__main__":

    wdir = Path(f"/tmp/raft_tests")
    if wdir.exists():
        shutil.rmtree(wdir)
    wdir.mkdir(parents=True)
    LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
    if LOGGING_TYPE != "silent":
        one_proc_log_setup(f"{wdir.as_posix()}/server.log")

    pc = PausingCluster(3)
    for server in pc.servers:
        # pause leader after new term record
        server.pause_on_substate(Substate.became_leader)
        # pause followers after they accept leader
        server.pause_on_substate(Substate.joined)
    pc.start_all()
    paused = []
    while len(paused) < 3:
        paused = []
        for server in pc.servers:
            if server.paused:
                paused.append(server)
        time.sleep(0.1)
    for server in pc.servers:
        server.clear_substate_pauses()
    for server in pc.servers:
        server.resume()
    print('All resumed')
    s1,s2,s3 = pc.servers
    print("Running three servers")
    client = s1.get_client()
    print(f"Query to {s1.name}")
    res = client.do_query()
    print(res)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('interrupt')
    finally:
        print("stopping servers")
        pc.stop_all()
    
    
