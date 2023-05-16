from .base_message import BaseMessage


class HeartbeatMessage(BaseMessage):

    _code = "heartbeat"

    def __init__(self, sender, receiver, term, data,
                 prevLogTerm, prevLogIndex, leaderCommit):
        self.prevLogTerm = prevLogTerm
        self.prevLogIndex = prevLogIndex
        self.leaderCommit = leaderCommit
        BaseMessage.__init__(self, sender, receiver, term, data)

    @classmethod
    def get_extra_fields(cls):
        return ["prevLogTerm", "prevLogIndex", "leaderCommit"]
        
class HeartbeatResponseMessage(BaseMessage):

    _code = "heartbeat_response"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        
