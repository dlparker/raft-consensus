

# Testing

Many of the tests use a scheme where one or more server
instances are run as independent threads using the MemoryBankTellerServer
class in see raft/dev_tools/bt_server.py or some child class
(see pausing_app.py).

# Coverage

## Coverage Exclusions

Things not covered in standard coverage setup, based on a judgement
call on what tests are worth the effort. Coverage exclusion is used
to make it easy to see how much testing has been done on the code that
really needs it, versus diluting the value of the coverage counted by
including useless items and accepting lower scores on important items.
By some philosophies this is an invalid approach. If you think that way,
just disable some of the exclude lines in the coverage.cfg file until
you are happy.

Some things that are excluded:
     
 - Errors likely only happen during shutdown, such as asyncio cancels
 - Errors that result from messages arriving to the wrong state, such
   as a vote reply arriving at a follower. This are not errors per se,
   as the code that receives them just ignores them. Setting up tests
   to cover this would be alot of work and would add almost nothing
   that simple inspection doesn't already provide. 
 - Errors of the sort that cause system to fail completely, such
   as a failure while trying to setup a UDP connection.
 - Errors in code that exists solely to support development and test,
   such as the memory and udp comms implementations. 
 - declarations of abstract methods on abstract classes and the
   ```__sbuclasshook__``` method
   



## Tricky bits:
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

   
