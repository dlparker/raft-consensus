from .base_message import BaseMessage


class ClientCommandMessage(BaseMessage):

    _code = "command"

    def __init__(self, sender, receiver, term, data, original_sender=None):
        BaseMessage.__init__(self, sender, receiver, term, data, original_sender)

class ClientCommandResultMessage(BaseMessage):

    _code = "command_result"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

