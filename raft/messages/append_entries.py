from .base_message import BaseMessage


class AppendEntriesMessage(BaseMessage):

    _code = "append_entries"

    def __init__(self, sender, receiver, term, data,
                 prevLogTerm, prevLogIndex, leaderCommit):
        self.prevLogTerm = prevLogTerm
        self.prevLogIndex = prevLogIndex
        self.leaderCommit = leaderCommit
        BaseMessage.__init__(self, sender, receiver, term, data)

    @classmethod
    def get_extra_fields(cls):
        return ["prevLogTerm", "prevLogIndex", "leaderCommit"]
        

class AppendResponseMessage(BaseMessage):

    _code = "append_response"

    def __init__(self, sender, receiver, term, data,
                 prevLogTerm, prevLogIndex, leaderCommit):
        # we echo back the details of the leader's message
        # so the leader can figure out what the original command
        # was
        self.prevLogTerm = prevLogTerm
        self.prevLogIndex = prevLogIndex
        self.leaderCommit = leaderCommit
        BaseMessage.__init__(self, sender, receiver, term, data)
        
    @classmethod
    def get_extra_fields(cls):
        return ["prevLogTerm", "prevLogIndex", "leaderCommit"]
