from .base_message import BaseMessage


class RequestVoteMessage(BaseMessage):

    code = "request_vote"

class RequestVoteResponseMessage(BaseMessage):

    code = "request_vote_response"

    def __init__(self, sender:str, receiver:str, term:int, prevLogIndex:int, prevLogTerm:int, vote:bool):
        BaseMessage.__init__(self, sender, receiver, term, prevLogIndex, prevLogTerm)
        self.vote = vote

    def __rep__(self):
        msg = super().__rep__()
        msg += f" v={self.vote}"
        return msg
