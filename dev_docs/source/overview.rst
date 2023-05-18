######################
Development Challenges
######################


There are two general classes of challenges that had to be addressed
to develop this library. One is the standard difficulty inherit in
dealing with systems of independent server process. The other is the
fact that the Raft algorithm is timeout based, such that the failure
of certain messages to arrive within a specified time window is
treated as the an actual failure of the sending server or the network
connection and triggers a new leader election.

Much energy has been expended to build tools to address these problems

************************
Single process execution
************************

A major element of the strategy to limit the pain of development and test
execution is to opt for running servers in a single process in mutliple
threads whenever that choice does not compromise the task at hand. Nearly
all test and development tasks can be executed this way.

One benefit of this approach is the ability to directly examine the
state of any one of the cluster's servers from within a single
process, which is very helpful in developing tests.

Another major benefit is the ability to cause pauses in the timeout
based processing so that all the servers in the cluster suspend
processing at some well defined state.

Combining these two features allows for complex test sequences. For
example, you can start some servers, wait for Leader election to
complete and cause all the servers to pause at that point. Then you
can examine or even modify the internal state of one or more of the
servers, and then allow the servers to resume processing without
triggering the timeout based parts of the algorithm.

Similarly you can start a cluster, wait for Leader election, then run
through some sequences of normal operations until suspending all
servers whe some desired state has reach, such as the AppendEntries
message having been sent but not yet acted upon by the Followers.
You can then alter some part of the state, stop the Leader server,
or something like that to cause some specific sequence of the algrorithm
to play out under test control.

The primary means of building in support for these extra control mechanism is to wrap
standard components (by inheritance) to add hooks and callbacks.

A secondary means is to replace standard components, as is done with the CommsAPI
implementation. 

===============
The Banking App
===============

An extremely simple banking app is used as the test and development support app,
:class:`dev_tools.bt_server.MemoryBankTellerServer`. It is
based on the banking logic that was part of the original project forked to start this one.
It supports query, credit and debit operations on a single account, using the RaftFrame
logic to accept updates only in the Leader server.


==================
Cluster Management
==================

There are common tools for managing clusters for test and development. There are two types of
clusters. One is based on python multiprocessing and uses the UDP comms module. The clusters
in a server of this type do not support any of the features that suspend operation, so it has
limited usefulness, largely just proving that normal flows work correctly when using UDP comms
in separate processes.

The other type is the "pausing app" flavor of cluster, with all servers run in different
threads in a single process, using a number of wrapper components including an in-memory
implementation of the CommsAP. 

-----------
Pausing App
-----------

The Pausing App is based on the class
:class:`dev_tools.bt_server.MemoryBankTellerServer`. This class sets
up the components and configuration needed to run a server, using the
:class:`raftframe.states.state_map.StandardStateMap`, which is the
default StateMap component intended for production use, but uses
special development implementations for the Log and Comms APIs, both
using memory structures to emulate storage and transport
operations. It uses the :class:`bank_app.BankingApp`
for the app, and it responds to calls to the start method by
starting a thread to run the server.


