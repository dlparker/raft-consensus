
# Raft Based Server Library

This code began life as a fork of
[baonguyen2604/raft-consensus](https://github.com/baonguyen2604/raft-consensus)
The code orignally forked was chosen because it is the most
understandable, straightforward implementation that provided enough of
the raft features, compared to the other python implementations
studied. A key motivator of the raft algorithm is to be sufficiently
understandable that it is likely to be implemented correctly. It seems
reasonable to take as a corollary the goal that the implementation should
also be easy to understand. The chosen fork base did that well.

However, the original code had features that seem to indicate that it
was just a proof of concept, not intended to be production code. This
fork therefore had to make a number of basic changes to eliminate those
features.

The other main goal of the change to this fork is to make it more of a
generalized library to aid in the construction of cluster of servers
rather that to turn it into another key-value store implemention. 

When building a server that is going to be part of a cluster that uses
raft for consensus, then changes in the cluster configuration should trigger
user code in order to allow decisions about possible processing algorithm adjustments.
This project is designed to provide a frame on which to build such servers.

Planned changes:
- Add functions to support integration in server clusters with control
  and status operations
- Add election state transition logging, in both python logging and in a user
  queryable format
- Add gRPC support
- Add startup pause/continue controls so that cluster management functions
  are possible, such as orderly shutdown, runtime reconfiguration, etc

Completed changes:
 - Changed a few variable names to make things a bit clearer
 - Addressed some minor coding issues to improve clarity and testing
 - Changed banking demo app to support actual multi-machine (or dockers/vms)
   ops, not local only
 - Added unit testing for existing code, at 98% coverage as of 5/24/23 on simplify branch
 - Added basic status query operation to report leader endpoint info to clients
 - Fixed logic bug in election code that caused failure when one server out
   of three failed.
 - Removed the embedded application code that provides the banking app
   functions, generalizing it to an API that the user code excersizes
 - Replaced the in memory log with at least one SQLite based log mechanism, and
   provided an API for other log mechanisms
 - Refactored where necessary to make it more clear how the message flow
   effects state management during elections
 - Replaced the existing UDP operations with a more general comms API,
   and re-implemented the UDP ops as a reference implementation

## References
- [raftos](https://github.com/zhebrak/raftos)
- [simpleRaft](https://github.com/streed/simpleRaft/tree/master/simpleRaft)
- [Raft visualization](http://thesecretlivesofdata.com/raft/)
