#!/usr/bin/env python
"""
"""
import os
import time
from dev_tools.udp_cluster import UDPServerCluster

LOGGING_TYPE=os.environ.get("TEST_LOGGING", None)
if LOGGING_TYPE is not None:
    LOGGING_TYPE = "devel_mp"



if __name__=="__main__":

    cluster = UDPServerCluster(3, logging_type=LOGGING_TYPE)
    cluster.prepare()
    cluster.start_all()
    s1,s2,s3 = cluster.get_servers().values()
    cluster.wait_for_state(server_name=s1.name)
    cluster.wait_for_state(server_name=s2.name)
    cluster.wait_for_state(server_name=s3.name)
    print("Running three servers")
    client = s1.get_client()
    print(f"Query to {s1.name}")
    res = client.do_query()
    print(res)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('interrupt')
    finally:
        print("stopping servers")
        cluster.stop_all()
    
    
