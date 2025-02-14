#!/usr/bin/env python
"""
An example of how to run a cluster and a client when using the in
memory comms, all of which have to be in the same process.

It sets up a cluster of three servers, then starts up a client loop.

"""
import os
import sys
import time
import asyncio
from pathlib import Path
import shutil
from raftframe.states.base_state import State, Substate
from dev_tools.pserver import PServer
from dev_tools.pcluster import PausingCluster
from dev_tools.log_control import one_proc_log_setup
from dev_tools.bt_client import MemoryBankTellerClient

def run_client(server_host, server_port):
    client = MemoryBankTellerClient(server_host, server_port)

    cmd = ''

    print('Welcome to your ATM! You can check your balance, credit to or debit from your account')
    print('Your starting balance is 0')
    print('Available commands are: query, credit <amount>, debit <amount>')

    while cmd != 'exit':
        cmd = input('Enter command: ')
        toks = cmd.split()
        if len(toks) == 0:
            continue
        if toks[0] =='query':
            result = client.do_query()
        elif toks[0] =='credit':
            result = client.do_credit(toks[1])
        elif toks[0] =='debit':
            result = client.do_debit(toks[1])
        elif toks[0] =='exit' or toks[0] == "quit":
            break
        else:
            print(f"confused by command {cmd}")
            continue
        print(result['response'])


if __name__=="__main__":

    wdir = Path(f"/tmp/raft_tests")
    if wdir.exists():
        shutil.rmtree(wdir)
    wdir.mkdir(parents=True)
    LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
    if LOGGING_TYPE != "silent":
        one_proc_log_setup(f"{wdir.as_posix()}/server.log")

    pc = PausingCluster(3)
    pc.start_all()
    try:
        run_client('localhost', 5000)
    except KeyboardInterrupt:
        pass
    finally:
        pc.stop_all()
    
        
    
    
