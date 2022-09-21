from .base_message import BaseMessage


class StatusQueryMessage(BaseMessage):

    _type = BaseMessage.StatusQuery

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)


class StatusQueryResponseMessage(BaseMessage):

    _type = BaseMessage.StatusQueryResponse

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

