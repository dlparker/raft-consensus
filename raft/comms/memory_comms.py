import time
import asyncio
import logging
import traceback
import abc

from typing import Union
from dataclasses import dataclass, field, asdict

from ..utils import task_logger 
from ..messages.serializer import Serializer
from .comms_api import CommsAPI

# this is for test support only
class MessageInterceptor(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def before_in_msg(self, message) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    async def after_in_msg(self, message) -> bool:
        raise NotImplementedError
        
    @abc.abstractmethod
    async def before_out_msg(self, message) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    async def after_out_msg(self, message) -> bool:
        raise NotImplementedError
        
queues = {}

def reset_queues():
    global queues
    queues = {}

def get_queues():
    global queues
    return queues
    
@dataclass
class Wrapper:
    data: bytes = field(repr=False)
    addr: tuple

class MemoryComms(CommsAPI):
    """ For testing, to allow multiple servers to run in the 
    same process and with tight control of the message flow
    """
    def __init__(self):
        self.channels = {}
        self.endpoint = None
        self.server = None
        self.queue = asyncio.Queue()
        self.task = None
        self.keep_running = False
        self.logger = logging.getLogger(__name__)
        self.interceptor = None

    def set_interceptor(self, interceptor: MessageInterceptor):
        self.interceptor = interceptor

    def get_interceptor(self) -> Union[MessageInterceptor, None]:
        return self.interceptor
        
    async def start(self, server, endpoint):
        self.endpoint = endpoint
        self.server = server
        global queues
        queues[endpoint] = self.queue
        self.logger.debug("starting %s full set is %s",
                          endpoint, list(queues.keys()))
        self.task = task_logger.create_task(self.listen(),
                                            logger=self.logger,
                                            message="comms listener error")
        self.keep_running = True

    async def stop(self):
        self.keep_running = False
        if self.task:
            self.task.cancel()
            await asyncio.sleep(0)

    def are_out_queues_empty(self):
        global queues
        any_full = False
        for name, queue in queues.items():
            if name == self.endpoint:
                continue
            if not queue.empty():
                any_full = True
        return not any_full
            
    async def post_message(self, message):
        global queues
        # make sure addresses are tuples
        message._sender = (message.sender[0], message.sender[1])
        message._receiver = (message.receiver[0], message.receiver[1])
        try:
            target = message.receiver
            # this can happen at startup, waiting for
            # other server threads to start
            if target not in queues:
                self.logger.info("%s not connected to %s", self.endpoint,
                                 target)
                return
            if self.interceptor:
                # let test code decide to pause things before
                # delivering
                self.logger.debug("calling interceptor before %s", message.code)
                deliver = await self.interceptor.before_out_msg(message)
            queue = queues[target]
            data = Serializer.serialize(message)
            w = Wrapper(data, self.endpoint)
            self.logger.debug("%s posted %s to %s",
                              self.endpoint, message.code, target)
            await queue.put(w)
            if self.interceptor:
                # let test code decide to pause things after
                # delivering
                self.logger.debug("calling interceptor after %s", message.code)
                deliver = await self.interceptor.after_out_msg(message)
        except Exception: # pragma: no cover error
            self.logger.error(traceback.format_exc())

    async def listen(self):
        global queues
        while self.keep_running:
            try:
                w = await self.queue.get()
                addr = w.addr
                data = w.data
                try:
                    message = Serializer.deserialize(data)
                    if self.interceptor:
                        # let test code decide to pause things before
                        # delivering
                        deliver = await self.interceptor.before_in_msg(message)
                except Exception as e:  # pragma: no cover error
                    self.logger.error(traceback.format_exc())
                    self.logger.error("cannot deserialze incoming data '%s...'",
                                      data[:30])
                    continue
                self.logger.debug("%s got %s from %s",
                                  self.endpoint, message.code, addr)
                # ensure addresses are tuples
                message._receiver = message.receiver[0], message.receiver[1]
                message._sender = message.sender[0], message.sender[1]
                await self.server.on_message(message)
                if self.interceptor:
                    # let test code decide to pause things after
                    # delivering
                    keep_going = await self.interceptor.after_in_msg(message)
            except asyncio.exceptions.CancelledError: # pragma: no cover error
                self.keep_running = False
                return
            except GeneratorExit: # pragma: no cover error
                self.keep_running = False
                return
            except Exception as e: # pragma: no cover error
                self.logger.error(traceback.format_exc())


