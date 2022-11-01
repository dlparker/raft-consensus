import os
from raft.dev_tools.bt_client import MemoryBankTellerClient
from raft.dev_tools.bt_client import UDPBankTellerClient
from raft.tests.common_test_code import BaseCase

    
class MemTestClientOps(BaseCase.TestClientOps):

    def get_logging_type(self):
        LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
        if LOGGING_TYPE != "silent":
            LOGGING_TYPE = "devel_one_proc" 
        return LOGGING_TYPE

    def get_process_flag(self):
        return False

    @property
    def use_log_pull(self):
        return True

    def get_client(self, port):
        return MemoryBankTellerClient("localhost", port)
        
    def get_loop_limit(self):
        loops = int(os.environ.get("TEST_LOOP_COUNT", 1))
        return loops

class UDPTestClientOps(BaseCase.TestClientOps):

    def get_logging_type(self):
        LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
        if LOGGING_TYPE != "silent":
            LOGGING_TYPE = "devel_mp" 
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
