from .base_message import BaseMessage


class AppendEntriesMessage(BaseMessage):

    _code = "append_entries"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)


class AppendResponseMessage(BaseMessage):

    _code = "append_response"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        
