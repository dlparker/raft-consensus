from .base_message import BaseMessage


class LogPullMessage(BaseMessage):

    _code = "log_pull"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

class LogPullResponseMessage(BaseMessage):

    _code = "log_pull_response"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        
