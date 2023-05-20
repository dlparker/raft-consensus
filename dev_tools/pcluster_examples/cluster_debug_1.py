#!/usr/bin/env python
"""
An example of how to run debugger from a paused server. This works well with pycharm
and pdb, but not ipdb.
"""
import os
import sys
import time
import asyncio
from raftframe.states.base_state import State, Substate
from dev_tools.pserver import PServer
from dev_tools.pcluster import PausingCluster

if __name__=="__main__":
    os.environ['PYTHONBREAKPOINT'] = 'pdb.set_trace'

    pc = PausingCluster(3)

    async def pause_callback(pserver):
        state = pserver.state_map.get_state()
        print(f'{pserver.name} {state} pausing')
        if str(state) == "leader":
            paused = []
            pc.debugging = True
            while len(paused) < 3:
                for server in pc.servers:
                    if server.paused:
                        paused.append(server)
                await asyncio.sleep(0.1)
                breakpoint()
                pc.debugging = False
                break
                
                
    async def resume_callback(pserver):
        print(f'{pserver.name} resumed')
        
    for server in pc.servers:
        server.pause_callback = pause_callback
        server.resume_callback = resume_callback
        # pause leader after new term record
        server.pause_on_substate(Substate.became_leader)
        # pause followers after they accept leader
        server.pause_on_substate(Substate.joined)
    pc.start_all()
    paused = []
    while len(paused) < 3:
        for server in pc.servers:
            if server.paused:
                paused.append(server)
        time.sleep(0.1)
    if pc.debugging:
        print("waiting for debug to end")
        while pc.debugging:
            time.sleep(0.1)
    print('All paused, resuming')
    for server in pc.servers:
        server.resume()
    print('All resumed')
    time.sleep(0.2)
    print('Stopping')
    for server in pc.servers:
        server.stop()
    stopped = []
    while len(stopped) < 3:
        for server in pc.servers:
            if not server.running:
                stopped.append(server)
        time.sleep(0.1)
    print('All stopped')
    
        
    
    
