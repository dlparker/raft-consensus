"""
How to add a new message type:

Extend the BaseMessage class, giving your new class a unique string value 
for the class variable "_code". Then add a couple of lines to the message registry 
"build_registry" module method. One to import your class, and another to 
register it, following the pattern there. Note that, if you supply the name of
a handler method, then you'll have to update the raftframe.states.base_state abstract
class to add the method, and fill in the relevant logic either there or in the
derived class for the state that can do something with the message. 


"""
from typing import Type

class BaseMessage:

    _code = "invalid"
    
    def __init__(self, sender, receiver, term, data,
                 original_sender=None):
        self._sender = sender
        self._receiver = receiver
        self._data = data
        self._term = term
        self._original_sender = original_sender
        self._msg_number = None

    def __str__(self):
        return f"{self._code} from {self._sender} to {self._receiver} term {self._term}"

    @classmethod
    def get_code(cls):
        return cls._code

    @classmethod
    def get_extra_fields(cls):
        return []

    def set_msg_number(self, number):
        self._msg_number = number
        
    def is_type(self, type_val):
        return self._code == type_val

    @property
    def code(self):
        return self._code
    
    @property
    def receiver(self):
        return self._receiver

    @property
    def sender(self):
        return self._sender

    @property
    def data(self):
        return self._data

    @property
    def term(self):
        return self._term
    

