from .base_message import BaseMessage


class HeartbeatMessage(BaseMessage):

    _code = "heartbeat"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

class HeartbeatResponseMessage(BaseMessage):

    _code = "heartbeat_response"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        
