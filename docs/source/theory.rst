####################
Theory of Operations
####################

********************
Clustering with Raft
********************

The basic feature that this library provides is a way to build server
clusters that coordinate their operations via the Raft algorithm, such
that each server can know the current distributed state of logged
data. This makes it possible to build clusters that manage data in a
distributed way.

As an example, imagine that you want to build an Internet Of Things
support tool that logs datastreams from devices, and you want to
spread the load of storing these streams across a cluster via some
sort of sharding method, where some property of the each data record
causes it to be directed to a specific server in the cluster to be
stored there rather than at all servers in the cluster.

It can be a simple matter to configure such a sharding method, but
only as long as all your servers are running. Once you start to
build robustness into the system to try to gain availability without
losing durability, you quickly start to approach the full problem
that Raft solves.

For example, suppose you decide to have at least one replica of each
data storage shard, such that at least two servers in the cluster
accept and save data for that shard. When you begin to design a
process for reorganizing cluster operations to account for the failure
of a server, you need to figure out which shard has lost one of its
replicas, and which server should take over that role. Whatever logic
you decide to use for this task, the result will be a reconfiguration
of your cluster. You need a way to coordinate the changes across the
whole cluster so that all servers have the same view of the cluster
configuration at any point in time.

This library allows you to build a cluster with the needed consistency
guarntees by building a Raft algorithm implementation directly into your
servers. You can then use the data log of the Raft cluster to store
your more general cluster configuration, and to query the current
configuration in order to make decisions about how to reconfigure
when servers are added or removed from the cluster.

If you'd like to understand the Raft algorithm, there is a copy of
the defining document at `<https://github.com/dlparker/raftframe/blob/master/raft.pdf>`_.
If you plan on building a server with this library, you should probably read
this until you at least understand the general concept of leader election.


********************
Basic Design
********************

The RaftFrame library includes a server class which connects the users
application code to the Raft implementation. The
:class:`raftframe.servers.server.Server` object listens for messages
from other cluster members and passes them to a state transision
manager class called a StateMap
:class:`raftframe.states.state_map.StateMap`.  This is not a true
state machine, but it does resemble one slightly.

There are individual state classes for each of the roles defined
in the Raft algorithm, namely Follower, Candidate, and Leader.

- :class:`raftframe.states.follower.Follower`
- :class:`raftframe.states.candidate.Candidate`
- :class:`raftframe.states.leader.Leader`

The server object passes messages it receives on to the currently
active state object, and it figures out what to do with the message
based on the algorithm's logic. These classes also implement timers
to monitor for the timeout conditions that trigger and sequence
leader election.

The StateMap class provides a couple of helper functions to connect
the server object and the state objects, but its main function is
to provide an isolated place for the code that performs the switching
process when one of the state objects decides that the server needs
to change to a different state. This code also supports some monitoring
functions that can be used as an event source to trigger code in other
places that cares about state switches.

Once started, the server must continue running with pausing because
it, through the current state object, must satisfy the requirements
for responsiveness built into the protocol. This is particularly
true of a server in the leader state, as it must send heartbeats
to the other servers on schedule or they will conclude that an
election is needed. Servers in the follower state have a lesser
need to remain responsive in normal operations, though can become
critical if an election is started.

Application developers must respect this and ensure that the async
event loop that runs the Raft operation is not blocked. This presents
significant challenges for devolpers trying to write tests or to
debug live code. Extensive development support code had to be written
to make these problems managable. See the developer documentation
for details.

For more details on the operations of the main components see
:ref:`main_components`.




