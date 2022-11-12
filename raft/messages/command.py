from .base_message import BaseMessage


class ClientCommandMessage(BaseMessage):

    _code = "command"

    def __init__(self, sender, receiver, term, data, original_sender=None):
        self.original_sender = original_sender
        BaseMessage.__init__(self, sender, receiver, term, data)

    @classmethod
    def get_extra_fields(cls):
        return ["original_sender",]

class ClientCommandResultMessage(BaseMessage):

    _code = "command_result"

    def __init__(self, sender, receiver, term, data, error=None):
        self.error = error
        BaseMessage.__init__(self, sender, receiver, term, data)

    @classmethod
    def get_extra_fields(cls):
        return ["error",]
