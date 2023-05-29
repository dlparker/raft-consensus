from typing import Union
import abc
import msgpack

from raftframe.messages.base_message import BaseMessage
from raftframe.log.log_api import LogRec

class SerializerAPI(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass):  # pragma: no cover abstract
        return (hasattr(subclass, 'serialize_dict') and 
                callable(subclass.serialize_dict) and
                hasattr(subclass, 'deserialize_dict') and 
                callable(subclass.deserialize_dict) and
                hasattr(subclass, 'serialize_message') and 
                callable(subclass.serialize_message) and
                hasattr(subclass, 'deserialize_message') and 
                callable(subclass.deserialize_message) and
                hasattr(subclass, 'serialize_logrec') and 
                callable(subclass.serialize_logrec) and
                hasattr(subclass, 'deserialize_logrec') and 
                callable(subclass.deserialize_logrec) and
                NotImplemented)

    @abc.abstractmethod
    def serialize_dict(user_dict: dict) -> Union[bytes, str]:
        raise NotImplementedError
        
    @abc.abstractmethod
    def deserialize_dict(data: Union[bytes, str]) -> dict:
        raise NotImplementedError
        
    @abc.abstractmethod
    def serialize_message(message: BaseMessage) -> Union[bytes, str]:
        raise NotImplementedError
        
    @abc.abstractmethod
    def deserialize_message(data: Union[bytes, str]) -> BaseMessage:
        raise NotImplementedError

    @abc.abstractmethod
    def serialize_logrec(rec: LogRec) -> Union[bytes, str]:
        raise NotImplementedError
        
    @abc.abstractmethod
    def deserialize_logrec(data: Union[bytes, str]) -> LogRec:
        raise NotImplementedError
    
    @abc.abstractmethod
    def deserialize_logrec(data: Union[bytes, str]) -> dict:
        raise NotImplementedError
    
