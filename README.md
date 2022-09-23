
# fork of [baonguyen2604/raft-consensus](https://github.com/baonguyen2604/raft-consensus)

The code orignally forked was chosen because it is the most
understandably, straightforward implementation that provided enough of
the raft features, compared to the other python implementations
chosen. A key motivator of the raft algorithm is to be sufficiently
understandable that it is likely to be implemented correctly. It seems
reasonable to take as a corollary the goal that the implementation should
also be easy to understand. The chosen fork base did that well.

However, the original code had features that seem to indicate that it
was just a proof of concept, not intended to be production code. This
fork must therefore make a number of basic changes to eliminate those
features.

The other main goal of the change to this fork is to make it more of a
generalized library to aid in the construction of cluster state management,
rather that to turn it into another key-value store implemention. The project
that is the motivation for wanting such a library is a simplified version of
the kafka stream processing system. One of the requirements of that project
is cluster logic that allows servers to coordinate their actions and deal with
cluster node failures and network partitions. The raft algorithm provides
proven methods for those strategies, but kafka like cluster logic requires
additional features. The simplest example is that unexpected changes in the
cluster configuration should trigger user code in order to allow decisions
about possible processing algorithm adjustments.

Planned changes:
- Add unit testing
- Add functions to support integration in server clusters with control
  and status operations
- Address some minor coding issues to improve clarity and testing
- Remove the embedded application code that provides the banking app
  functions, generalizing it to an API that the user code excersizes
- Replace the in memory log with at least one persistent log mechanism, and
  provide an API for other log mechanisms
- Refactor where necessary to make it more clear how the message flow
  effects state management during elections
- Add election state transition logging, in both python logging and in a user
  queryable format
- Replace the existing UDP operations with a more general comms API,
  and re-implement the UDP ops as a reference implementation
- Add gRPC support
- Add startup pause/continue controls so that cluster management functions
  are possible, such as orderly shutdown, runtime reconfiguration, etc

Completed changes:
 - Changed a few variable names to make things a bit clearer
 - Changed banking demo app to support actual multi-machine (or dockers/vms)
   ops, not local only
 - Added unit testing for existing code, at 89% coverage as of 9/23/22
 - Added basic status query operation to report leader endpoint info to clients
 - Fixed logic bug in election code that caused failure when one server out
   of three failed.

## References
- [raftos](https://github.com/zhebrak/raftos)
- [simpleRaft](https://github.com/streed/simpleRaft/tree/master/simpleRaft)
- [Raft visualization](http://thesecretlivesofdata.com/raft/)
