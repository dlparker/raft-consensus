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

import asyncio
import time
import logging
import traceback

from raft.dev_tools.log_control import config_logging

from raft.dev_tools.udp_cluster import UDPServerCluster
from raft.dev_tools.bt_client import UDPBankTellerClient

config,_ = config_logging("test.log")
dictConfig(config)

def test_leader_stop():
    logger = logging.getLogger(__name__)
    logger.info("starting test_leader_stop")
    client1 =  UDPBankTellerClient("localhost", 5000)
    status = None
    logger.info("waiting for ready")
    start_time = time.time()
    while time.time() - start_time < 3:
        time.sleep(0.25)
        status = client1.get_status()
        if status and status.data['leader']:
            break
    if not status:
        raise Exception('no status')
    if not status.data['leader']:
        raise Exception('no leader')

    leader_addr = status.data['leader']
    leader = None
    first_follower = None
    second_follower = None
    server_recs = {}
    for i in range(3):
        port = 5000 + i
        name = f"server_{i}"
        server_recs[name] = dict(name=name, port=port)
    for name,sdef in server_recs.items():
        if sdef['port'] == leader_addr[1]:
            sdef['role'] = "leader"
            leader = sdef
        else:
            if not first_follower:
                first_follower = sdef
            else:
                second_follower = sdef
            sdef['role'] = "follower"
    logger.info("found leader %s", leader_addr)

    balance = client1.do_query()
    cur_balance = int(balance.split(":")[-1].strip())
    if cur_balance != 0:
        logger.info("clearing existing balance of %d", cur_balance)
        client1.do_debit(cur_balance)
    client1.do_credit(10)
    balance = client1.do_query()
    if balance != "Your current account balance is: 10":
        raise Exception(f"client1 bad balance {balance}")
    print(balance)
    logger.info("initial call to 5000 worked")

    # get a client for the first follower
    client2 =  UDPBankTellerClient("localhost", 
                                   first_follower['port'])
    balance = client2.do_query()
    if balance != "Your current account balance is: 10":
        raise Exception(f"client2 bad balance {balance}")
    logger.info("call to 5001 worked")
    print(f"now stop server, leader is {leader_addr}")
    input("Press Enter to continue...")
    if leader['port'] == 5000:
        new_client = client2
    else:
        new_client = client1
    # wait for election to happen
    logger.info("waiting for election results")
    start_time = time.time()
    while time.time() - start_time < 7:
        time.sleep(0.25)
        status = new_client.get_status()
        if status:
            new_leader_addr = status.data['leader']
            if (new_leader_addr
                and new_leader_addr[0] != -1 &
                new_leader_addr[1] != leader_addr[1]):
                break
    if new_leader_addr[0] == -1:
        print(status)
        raise Exception(f"no leader")
    if new_leader_addr[1] == leader_addr[1]:
        print(status)
        raise Exception(f"election never happend")
    logger.info("new leader found %s", new_leader_addr)
    balance = new_client.do_query()
    if balance != "Your current account balance is: 10":
        raise Exception(f"new_client bad balance {balance}")
    new_client.do_credit(10)
    balance = new_client.do_query()
    if balance != "Your current account balance is: 20":
        raise Exception(f"new_client bad balance {balance}")
    logger.info("after leader dead, balance correct and new credit worked")
    return


if __name__=="__main__":
    test_leader_stop()
