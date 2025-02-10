from .base_message import BaseMessage


class RequestVoteMessage(BaseMessage):

    _code = "request_vote"

    def __init__(self, sender, receiver, term, data,
                 prevLogTerm, prevLogIndex, leaderCommit):
        self.prevLogTerm = prevLogTerm
        self.prevLogIndex = prevLogIndex
        self.leaderCommit = leaderCommit
        BaseMessage.__init__(self, sender, receiver, term, data)

    @classmethod
    def get_extra_fields(cls):
        return ["prevLogTerm", "prevLogIndex", "leaderCommit"]

    def __str__(self):
        return f"{self._sender} wants vote from {self._receiver} term {self._term}"

class RequestVoteResponseMessage(BaseMessage):

    _code = "request_vote_response"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

    def __str__(self):
        return f"{self._receiver} votes {self._data['response']} for {self._sender} term {self._term}"
