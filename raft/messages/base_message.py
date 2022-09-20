class BaseMessage(object):
    AppendEntries = 'append_entries'
    RequestVote = 'request_vote'
    RequestVoteResponse = 'request_vote_response'
    Response = 'response'
    StatusQuery = 'status_query'
    StatusQueryResponse = 'status_query_response'

    def __init__(self, sender, receiver, term, data):
        self._sender = sender
        self._receiver = receiver
        self._data = data
        self._term = term

    def __str__(self):
        return f"{self._type} {self._sender} {self._receiver} {self._term} {self._data}"
    
    @property
    def receiver(self):
        return self._receiver

    @property
    def sender(self):
        return self._sender

    @property
    def data(self):
        return self._data

    @property
    def term(self):
        return self._term

    @property
    def type(self):
        return self._type
