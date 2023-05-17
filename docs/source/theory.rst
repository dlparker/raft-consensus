####################
Theory of Operations
####################

********************
Clustering with Raft
********************

The basic feature that this library provides is a way to build server
clusters that coordinate their operations via the raft algorithm, such
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
that raft solves.

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
guarntees by building a raft algorithm implementation directly into your
servers. You can then use the data log of the raft cluster to store
your more general cluster configuration, and to query the current
configuration in order to make decisions about how to reconfigure
when servers are added or removed from the cluster.




