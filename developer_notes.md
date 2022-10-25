

# Testing

Tricky bits:
 - UDP client and coverage:
   There is some sort of issue with the way that the UDP tests environment
   and coverage interact. Sometimes (timing dependent), if one of the
   test target UDP servers is not running when you first try to send
   a status query packet, then no packets ever get through to the server.
   If you have this condition, it will not happen if you turn coverage
   off. The workaround is to wait for the server to start. You can see
   how this is done in raft/tests/bt_server.py and raft/tests/udp_cluster.py
   using a multiprocess Manager. The cluser code has a "wait_for_state",
   method that makes it easy to wait for one or all servers to start.
   
   
