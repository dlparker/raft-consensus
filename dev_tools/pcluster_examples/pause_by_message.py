#!/usr/bin/env python
"""
An example of how to run a cluster and do something when pausing happens.

It sets up a cluster of three servers, then configures them to pause 
on completion of leader election, and to call our pause_callback 
async function once paused. Then it starts the servers, waits for them
all to pause. At that point the mainline code waits for a flag to 
be set and then tells all servers to resume. It then waits a bit and
then tells them all to stop.

You would probably want to write some code in the pause_callback 
to do something interesting. Here we dump the state of the 
server.

"""
import os
import sys
import time
import asyncio
from pathlib import Path
import shutil
from raftframe.states.base_state import State, Substate
from raftframe.messages.heartbeat import HeartbeatMessage
from dev_tools.pserver import PServer
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
    go_flag = False
    async def pause_callback(pserver, context):
        global go_flag
        state = pserver.state_map.get_state()
        print(f'{pserver.name} {state} pausing')
        if str(state) == "leader":
            print("I am paused in leader server")
        elif str(state) == "candidate":
            print("I am paused in candidate server")
        elif str(state) == "follower":
            print("I am paused in follower server")

        log = pserver.thread.server.get_log()
        print("*"*100)
        print(f"Server {pserver.name} log stats\n" 
              f"term = {log.get_term()}, last_rec_term = {log.get_last_term()}\n" 
              f"last_rec_index = {log.get_last_index()}, commit = {log.get_commit_index()}")
        print("*"*100)
        go_flag = True
        
    async def resume_callback(pserver):
        print(f'{pserver.name} resumed')
        
    for server in pc.servers:
        server.pause_callback = pause_callback
        server.resume_callback = resume_callback
        # pause followers when they get first heartbeat
        server.pause_before_in_message(HeartbeatMessage.get_code())
        # pause leader after all heartbeats sent
        server.pause_on_substate(Substate.sent_heartbeat)
    pc.start_all()
    paused = []
    while len(paused) < 3:
        paused = []
        for server in pc.servers:
            if server.paused:
                paused.append(server)
        time.sleep(0.1)
    print('All paused, awaiting go flag')
    while not go_flag:
        time.sleep(0.1)
    print('Got go flag, resuming')
    for server in pc.servers:
        server.clear_message_triggers()
        server.clear_substate_pauses()
    for server in pc.servers:
        server.resume()
    print('All resumed')
    time.sleep(0.2)
    print('Stopping')
    for server in pc.servers:
        server.stop()
    stopped = []
    while len(stopped) < 3:
        stopped = []
        for server in pc.servers:
            if not server.running:
                stopped.append(server)
        time.sleep(0.1)
    print('All stopped')
    
        
    
    
