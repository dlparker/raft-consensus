from .base_message import BaseMessage


class TermStartMessage(BaseMessage):

    _type = BaseMessage.TermStart

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

class TermStartResponseMessage(BaseMessage):

    _type = BaseMessage.TermStartResponse

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        
