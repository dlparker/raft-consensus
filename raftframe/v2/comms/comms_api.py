import abc
import asyncio
import logging
import traceback


class CommsAPI(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass): # pragma: no cover abstract
        
        return (hasattr(subclass, 'post_message') and 
                callable(subclass.post_message) or
                NotImplemented)

    @abc.abstractmethod
    async def post_message(self, message): # pragma: no cover abstract
        raise NotImplementedError
