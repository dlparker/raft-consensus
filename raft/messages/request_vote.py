from .base_message import BaseMessage


class RequestVoteMessage(BaseMessage):

    _type = BaseMessage.RequestVote

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

    def __str__(self):
        return f"{self._type} from {self._sender} to {self._receiver} term {self._term} data {self._data}"

class RequestVoteResponseMessage(BaseMessage):

    _type = BaseMessage.RequestVoteResponse

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

    def __str__(self):
        return f"{self._type} from {self._sender} to {self._receiver} term {self._term} data {self._data}"
