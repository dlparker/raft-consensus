from .base_message import BaseMessage


class StatusQueryMessage(BaseMessage):

    _code = "status_query"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)


class StatusQueryResponseMessage(BaseMessage):

    _code = "status_query_response"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

