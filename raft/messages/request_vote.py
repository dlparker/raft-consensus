from .base_message import BaseMessage


class RequestVoteMessage(BaseMessage):

    _code = "request_vote"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

    def __str__(self):
        return f"{self._code} from {self._sender} to {self._receiver} term {self._term} data {self._data}"

class RequestVoteResponseMessage(BaseMessage):

    _code = "request_vote_response"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

    def __str__(self):
        return f"{self._code} from {self._sender} to {self._receiver} term {self._term} data {self._data}"
