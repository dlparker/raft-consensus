"""
How to add a new message type:

Extend the BaseMessage class, giving your new class a unique string value 
for the class variable "_code". 

"""
from typing import Type

class BaseMessage:

    _code = "invalid"
    
    def __init__(self, sender:str, receiver:str, term:int, prevLogIndex:int, prevLogTerm:int):
        self.sender = sender
        self.receiver = receiver
        self.term = term
        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm

    def __str__(self):
        return self.__rep__()

    def __rep__(self):
        msg = f"{self.code}:{self.sender}->{self.receiver}: "
        msg += f"t={self.term},pI={self.prevLogIndex},pt={self.prevLogTerm}"
        return msg
    
    @classmethod
    def get_code(cls):
        return cls.code

    def is_type(self, type_val):
        return self._code == type_val

    

