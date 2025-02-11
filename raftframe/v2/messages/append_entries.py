from typing import Any, List
from .base_message import BaseMessage


class AppendEntriesMessage(BaseMessage):

    code = "append_entries"

    def __init__(self, sender:str, receiver:str, term:int, prevLogIndex:int, prevLogTerm:int,
                 entries:List[Any]):
        BaseMessage.__init__(self, sender, receiver, term, prevLogIndex, prevLogTerm)
        self.entries = entries
    
    def __rep__(self):
        msg = super().__rep__()
        msg += f" e={len(self.entries)}"
        return msg

class AppendResponseMessage(BaseMessage):

    code = "append_response"

    def __init__(self, sender:str, receiver:str, term:int, prevLogIndex:int, prevLogTerm:int,
                 entries:List[Any], results:List[Any],
                 myPrevLogIndex:int, myPrevLogTerm:int):
        BaseMessage.__init__(self, sender, receiver, term, prevLogIndex, prevLogTerm)
        self.myPrevLogIndex = myPrevLogIndex 
        self.myPrevLogTerm = myPrevLogTerm
        self.entries = entries
        self.results = results
    
    def __rep__(self):
        msg = super().__rep__()
        msg += f" e={len(self.entries)} r={len(self.results)}"
        return msg

