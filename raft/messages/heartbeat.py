from .base_message import BaseMessage


class HeartbeatMessage(BaseMessage):

    _type = BaseMessage.Heartbeat

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

class HeartbeatResponseMessage(BaseMessage):

    _type = BaseMessage.HeartbeatResponse

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        
