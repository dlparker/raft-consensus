import abc
import asyncio
import logging
import traceback
from dataclasses import dataclass, field

@dataclass
class Endpoint:
    protocol: str
    details: dict = field(default={})

    
class CommsAPI(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass): # pragma: no cover abstract
        
        return (hasattr(subclass, 'start') and 
                callable(subclass.start) and
                hasattr(subclass, 'stop') and 
                callable(subclass.stop) and
                hasattr(subclass, 'post_message') and 
                callable(subclass.post_message) or
                NotImplemented)

    @abc.abstractmethod
    async def start(self, msg_callback, endpoint: Endpoint): # pragma: no cover abstract
        raise NotImplementedError

    @abc.abstractmethod
    async def get_endpoint(self) -> Endpoint:
        raise NotImplementedError
    
    @abc.abstractmethod
    async def get_endpoint_string(self) -> str:
        raise NotImplementedError
    
    @abc.abstractmethod
    async def stop(self): # pragma: no cover abstract
        raise NotImplementedError
    
    @abc.abstractmethod
    async def post_message(self, message): # pragma: no cover abstract
        raise NotImplementedError

    @abc.abstractmethod
    async def get_message(self): # pragma: no cover abstract
        raise NotImplementedError

    @abc.abstractmethod
    def are_out_queues_empty(self):
        raise NotImplementedError
