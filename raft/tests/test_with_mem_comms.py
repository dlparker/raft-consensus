import unittest
import asyncio
import time
import logging
import traceback
import os

from raft.tests.timer import get_timer_set
from raft.tests.setup_utils import Cluster
from raft.tests.bt_client import UDPBankTellerClient, MemoryBankTellerClient
from raft.tests.common_test_code import BaseCase
from raft.states.log_api import LogRec
from raft.states.memory_log import MemoryLog
from raft.states.follower import Follower
from raft.messages.regy import get_message_registry

#LOGGING_TYPE = "devel_one_proc" when using Mem comms and thread based servers
#LOGGING_TYPE = "devel_mp" when using UDP comms and MP process based servers
#LOGGING_TYPE = "silent" for no log at all
LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")

if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc" 
    
class MemTestThreeServers(BaseCase.TestThreeServers):

    def get_logging_type(self):
        return LOGGING_TYPE

    def get_process_flag(self):
        return False

    def get_client(self, port):
        return MemoryBankTellerClient("localhost", port)
        
