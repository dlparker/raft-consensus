from .base_message import BaseMessage


class TermStartMessage(BaseMessage):

    _code = "term_start"

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)

        
