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

***************
Pausing Servers
***************

The customer servers used for the threaded, single process cluster
implement a "pause" by entering an async wait loop waiting until a
resume flag is set somehow. Since this is async, other things will
continue to happen, and some of those things will change the state of
the server. Timer base operations are the primary example. The state
classes use timers to trigger elements of the Raft protocol. The
Leader uses a timer to send heartbeats, and the followers use a timer
to ensure that a heartbeat has arrived in less than the specified time
limit, else it starts an election. The Candidate uses a timer to
detect whether the voting period has expired without resolution and
switches back to Follower state.

Messages can also defeat the idea of being paused. For example, if a Candidate
receives a heartbeat from a Leader and the state is valid, then the Cadidate
switches back to a Follower.

In order to suspend servers at some desired state, these async events need
to be prevented, or delayed more likely. The timers need to be paused any
time that the server pauses, for whatever reason. Depending on the state
of the servers when the pause begins, it may also be necessary to interrupt
message processing to prevent state changes.

--------------
Pausing timers
--------------

The three state classes, Follower, Candidate and Leader all schedule callbacks
from timers in order to detect and respond to changes in cluster state. These
timer callbacks need to be delayed during any pause. These suspendable servers
therefore use a special timer class that can be paused, and that is cataloged
in a way that it is easy to find all the timers in a thread and pause and
resume them. This is accomplished by replacing all timer instances with
instances of the :class:`dev_tools.timer_wrapper.ControlledTimer` class,
which registers itself with the :class:`dev_tools.timer_wrapper.TimerSet`
class, an instance of which is stored in thread local storage, so each
thread has a TimerSet instance that has a reference to every ControlledTimer.

The method of replacing the normal timer class with the ControlledTimer is
based on the fact that the state classes setup their timers by calling the
get_timer or get_timer_class method on the :class:`raftframe.servers.server.Server`
class. This makes it simple to substitute the ControlledTimer for the whole
server without complicating the initialization of the state classes.

-------------
Pausing Comms
-------------

The :class:dev_tools.memory_comms.MemoryComms class implements
the CommsAPI, but adds an "interceptor" feature, such that it can be dynamically
configured to pause just before or just after sending or just before or just after
receiving (processing, actually) a message of a particular type.

So, for example, you can configure all the servers to stop after sending the heartbeat
message and before processing a received heartbeat message. If the server state is
Leader, then it will send the heartbeat and then pause. If a server receives this message
it will pause before processing it. Note that this example will result in only one server
pausing before processing, so if the cluster size > 2 then the other servers will eventually
timeout waiting for the heartbeat, so care must be taken to work around this issue.

As an extra option the pre-send and pre-processing interceptors functions can return False, thereby
telling the MemoryComms code not to send or process the targeted message.

This interceptor based pausing can be used in conjunction with other pausing techniques
to ensure that a paused server does not process an incoming message while in the paused state.

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
implementation of the CommsAPI. 


------------
Test Servers
------------

There are two test server base classes that are intended to run in the two types of cluster setups,
multiprocessing/UDP and threaded/Memory. Both use the :class:`bank_app.BankingApp`
as the App API implementation.

The :class:`dev_tools.bt_server.UDPBankTellerServer` the in memory log implementation
:class:`dev_tools.memory_log.MemoryLog`. It also uses a special StateMap implementation
that records current state and substate information via a python multiprocessing
manager instance, thus allowing test code to check the state and substate values for
subprocesses. The class has a class method that provides the multiprocess process startup
sequence.

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



