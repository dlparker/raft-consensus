from typing import Union
import abc
import msgpack

from raft.messages.base_message import BaseMessage
# abstract class for all server states
class SerializerAPI(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass):  # pragma: no cover abstract
        return (hasattr(subclass, 'serialize') and 
                callable(subclass.serialize) and
                hasattr(subclass, 'deserialize') and 
                callable(subclass.deserialize) or
                NotImplemented)

    @abc.abstractmethod
    def serialize_message(message: BaseMessage) -> Union[bytes, str]:
        raise NotImplementedError
        
    @abc.abstractmethod
    def deserialize_message(data: Union[bytes, str]) -> BaseMessage:
        raise NotImplementedError
