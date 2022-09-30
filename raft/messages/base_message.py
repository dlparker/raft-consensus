from typing import Type

class BaseMessage(object):

    def __init__(self, sender, receiver, term, data, original_sender=None):
        self._sender = sender
        self._receiver = receiver
        self._data = data
        self._term = term
        self._original_sender = original_sender

    def __str__(self):
        return f"{self._code} from {self._sender} to {self._receiver} term {self._term}"

    @classmethod
    def get_code(cls):
        return cls._code
    
    def is_type(self, type_val):
        return self._code == type_val

    def props_as_dict(self):
        return dict(code=self._code,
                    sender=self._sender,
                    receiver=self._receiver,
                    term=self._term,
                    data=self._data)                      

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

    @property
    def original_sender(self):
        return self._original_sender
