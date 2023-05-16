#!/usr/bin/env python
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path

from dev_tools.udp_cluster import UDPServerCluster

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    LOGGING_TYPE = "devel_mp"

timeout_basis = 1.0

class Looper:
    
    def one_loop(self):
        self.cluster = UDPServerCluster(server_count=3,
                                        logging_type=LOGGING_TYPE,
                                        base_port=5000)
        self.logger = logging.getLogger(__name__)
    
        # Test the pause support for just after election is done
        servers = self.cluster.prepare(timeout_basis=timeout_basis)
        self.cluster.start_all_servers()
        self.logger.info("waiting for election results")
        start_time = time.time()
        spec = servers["server_0"]
        time.sleep(0.1)
        client = spec.get_client()
        status = None
        while time.time() - start_time < 3:
            try:
                status = client.get_status()
                if status:
                    if status.data['leader'] is not None:
                        break
            except:
                time.sleep(0.1)
                pass
        if status is None or status.data['leader'] is None:
            raise Exception("election did not happen")

        client.do_credit(10)
        client.do_debit(5)
        result = client.do_query()
        if result['balance'] != 5:
            raise Exception("credit/debit/query did not work")
        
        self.cluster.stop_all_servers()

    def do_loops(self, how_many=1):
        for i in range(how_many):
            self.one_loop()

if __name__=="__main__":

    import sys
    looper = Looper()
    looper.do_loops(int(sys.argv[1]))

                 
    
            
