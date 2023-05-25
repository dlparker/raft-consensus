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
    client = s1.get_client()
    print(f"Query to {s1.name}")
    res = client.do_query()
    print(res)
    print(f"Credit to to {s1.name}")
    res = client.do_credit(10)
    print(res)
    print(f"Query to {s1.name}")
    res = client.do_query()
    print(res)
    cluster.stop_all()
    
    
