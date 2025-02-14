
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
features. Eventually it underwent a complete rewrite. The only things
remaining are:

1. The structure of the message classes, where the specific messages
   derive from a base class, and the names of the specific messages,
   which in turn derive from the original paper found in raft.pdf.
2. The structure of the state classes, where the specific states
   derive from a base class.
3. The names of the mmessages and states directories.

As a result of the extent of these changes, I have changed the license
file to include me as copyright holder, and removed Bao Nguyen's name.
I am not a lawyer, but I have studdied software copyright litigation,
and my laymen's assessment is that nothing I retained constitutes copying
of the original. 

The main purpose of this project is to make it a generalized library
to aid in the construction of cluster of servers. 

When building a server that is going to be part of a cluster that uses
raft for consensus, then changes in the cluster configuration should trigger
user code in order to allow decisions about possible processing algorithm adjustments.
This project is designed to provide a frame on which to build such servers.

## References

- [raftos](https://github.com/zhebrak/raftos)
- [protocol extensions]https://dev.to/tarantool/raft-notalmighty-how-to-make-it-more-robust-3a11
- [simpleRaft](https://github.com/streed/simpleRaft/tree/master/simpleRaft)
- [Raft visualization](http://thesecretlivesofdata.com/raft/)
