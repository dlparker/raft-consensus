#!/usr/bin/env python
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path


from raft.tests.bt_client import MemoryBankTellerClient
from raft.tests.ps_cluster import PausingServerCluster, PausePoint

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_one_proc"

timeout_basis = 0.2

class Looper:
    
    def one_loop(self):
        self.cluster = PausingServerCluster(server_count=3,
                                            logging_type=LOGGING_TYPE,
                                            base_port=5000)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
                
        self.logger = logging.getLogger(__name__)
    
        # Test the pause support for just after election is done
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.add_pause_point(PausePoint.election_done)
        self.cluster.start_all_servers()
        self.logger.info("waiting for election results")
        paused = self.cluster.wait_for_pause()
        if not paused:
            raise Exception("did not pause")
        self.cluster.resume_from_stepper_pause()

        # make sure it works after resume
        client = MemoryBankTellerClient("localhost", 5000)
        status = client.get_status()
        if status is None or status.data['leader'] is None:
            raise Exception("client didn't work after pause")
        self.cluster.stop_all_servers()
        time.sleep(0.1)
        self.loop.close()

    def do_loops(self, how_many=1):
        for i in range(how_many):
            self.one_loop()

if __name__=="__main__":

    import sys
    looper = Looper()
    looper.do_loops(int(sys.argv[1]))

                 
    
            
