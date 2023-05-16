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

from dev_tools.log_control import config_logging

from dev_tools.bt_client import UDPBankTellerClient

config,_ = config_logging("test.log")
dictConfig(config)

def test_leader_stop():
    logger = logging.getLogger(__name__)
    logger.info("starting transaction loop")
    client1 =  UDPBankTellerClient("localhost", 5000)
    result = client1.do_query()
    print(result)
    start_balance = result['balance']
    client1.do_credit(10)
    result = client1.do_query()
    print(result)
    if result['balance'] != start_balance + 10:
        raise Exception(f"client1 bad balance {result['balance']} not {start_balance + 10}")
    client1.do_debit(10)
    result = client1.do_query()
    print(result)
    if result['balance'] != start_balance:
        raise Exception(f"client1 bad balance {balance} not {start_balance}")

    return


if __name__=="__main__":
    test_leader_stop()
