import os
import multiprocessing

from raft.dev_tools.bt_client import UDPBankTellerClient
from raft.tests.common_test_code import BaseCase

#LOGGING_TYPE = "devel_one_proc" when using Mem comms and thread based servers
#LOGGING_TYPE = "devel_mp" when using UDP comms and MP process based servers
#LOGGING_TYPE = "silent" for no log at all
LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")

if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_mp"
    
class UDPTestThreeServers(BaseCase.TestThreeServers):

    def get_logging_type(self):
        return LOGGING_TYPE

    def get_process_flag(self):
        return True

    @property
    def use_log_pull(self):
        return True

    def get_client(self, port):
        return UDPBankTellerClient("localhost", port)
        
    def get_loop_limit(self):
        loops = int(os.environ.get("TEST_LOOP_COUNT", 1))
        return loops
