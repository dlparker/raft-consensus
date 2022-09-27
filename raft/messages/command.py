from .base_message import BaseMessage


class ClientCommandMessage(BaseMessage):

    _type = BaseMessage.ClientCommand

    def __init__(self, sender, receiver, term, data, original_sender=None):
        BaseMessage.__init__(self, sender, receiver, term, data, original_sender)

class ClientCommandResultMessage(BaseMessage):

    _type = BaseMessage.ClientCommandResponse

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

