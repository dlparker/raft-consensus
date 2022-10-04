
from raft.servers.server import Server
from raft.states.base_state import State
from raft.states.follower import Follower
from raft.states.Candidate import Candidate
from raft.states.leader import Leader
from raft.log.memory_log import MemoryLog

# Wrapper classes that do useful test support things
# when running in single proc, threaded, MemoryComms
# based mode.
class ServerWrapper(Server):
    pass

class StateWrapper(State):
    pass

class FollowerWrapper(Follower):
    pass

class CandidateWrapper(Candidate):
    pass

class LeaderWrapper(Leader):
    pass

class LogWrapper(MemoryLog)
    pass
