import time
import asyncio
import logging
import traceback
import random
import abc

from typing import Union
from dataclasses import dataclass, field, asdict

from raftframe.utils import task_logger
#from raftframe.serializers.msgpack import MsgpackSerializer as Serializer
from raftframe.comms.comms_api import CommsAPI

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

channels = {}
clients = {}

def reset_channels():
    global channels
    channels = {}

def get_channels():
    global channels
    return channels

def add_client(queue):
    global channels
    global clients
    limit = 5000 - 100  #usual server base port minus some
    poss = ('localhost', int(random.uniform(0, limit)))
    while (poss in channels or poss in clients): # pragma: no cover
        poss = ('localhost', int(random.uniform(0, limit)))
    client = ClientComms(poss, queue)
    clients[poss] = client
    return client
            
class ClientComms:

    def __init__(self, addr, queue):
        self.addr = addr
        self.queue = queue
        
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
        self.serializer = None
        self.queue = asyncio.Queue()
        self.task = None
        self.keep_running = False
        self.logger = logging.getLogger(__name__)
        self.interceptor = None
        self.pause_new = False
        self.partition = 0 # used to support faked network partitioning

    def set_interceptor(self, interceptor: MessageInterceptor):
        self.interceptor = interceptor

    def get_interceptor(self) -> Union[MessageInterceptor, None]:
        return self.interceptor

    def set_partition(self, value):
        self.partition = value

    async def start(self, server, endpoint):
        self.endpoint = endpoint
        self.server = server
        self.serializer = self.server.get_serializer()
        global channels
        channels[endpoint] = self
        self.logger.debug("starting %s full set is %s",
                          endpoint, list(channels.keys()))
        self.task = task_logger.create_task(self.listen(),
                                            logger=self.logger,
                                            message="comms listener error")
        self.keep_running = True

    async def stop(self):
        self.keep_running = False
        if self.task:
            self.task.cancel()
            await asyncio.sleep(0)

    def pause_new_messages(self):
        self.pause_new = True

    def resume_new_messages(self):
        self.pause_new = False
    
    def are_out_queues_empty(self):
        global channels
        any_full = False
        for name, channel in channels.items():
            if name == self.endpoint:
                continue
            if not channel.queue.empty():
                any_full = True
        return not any_full
            
    async def post_message(self, message):
        global channels
        global clients
        # make sure addresses are tuples
        message._sender = (message.sender[0], message.sender[1])
        message._receiver = (message.receiver[0], message.receiver[1])
        while self.pause_new:
            await asyncio.sleep(0.01)
        try:
            target = message.receiver
            # this can happen at startup, waiting for
            # other server threads to start
            if self.interceptor:
                # let test code decide to pause things before
                # delivering
                self.logger.debug("calling interceptor before %s",
                                  message.code)
                deliver = await self.interceptor.before_out_msg(message)
                if not deliver:
                    self.logger.info("not delivering posted message "\
                                      "because interceptor said so")
                    return
            if target in channels or target in clients:
                skip = False
                channel = channels.get(target, None)
                if channel is None:
                    client = clients[target]
                    queue = client.queue
                else:
                    queue = channel.queue
                    if channel.partition != self.partition:
                        self.logger.info("%s is in partition %d, we are %d" \
                                          " not sending %s so as to look like"
                                          " network partition",
                                          target, channel.partition,
                                          self.partition, message.code)
                        skip = True
                if not skip:
                    data = self.serializer.serialize_message(message)
                    w = Wrapper(data, self.endpoint)
                    self.logger.info("%s posted %s to %s",
                                      self.endpoint, message.code, target)
                    await queue.put(w)
            else:
                if target[1] < 5000:
                    self.logger.info("\n\n\t%s not in %s\n\n",
                                     target, list(clients.keys()))
                self.logger.info("%s not connected to %s," \
                                 " not sending %s", self.endpoint,
                                 target, message.code)
            if self.interceptor:
                # We want to call the interceptor regardless of
                # whether we could deliver the message or not,
                # as it may be doing counting of messages for things
                # such as heartbeats to ensure it has sent to all
                # followers before pausing
                self.logger.debug("calling interceptor after %s", message.code)
                await self.interceptor.after_out_msg(message)
        except Exception: # pragma: no cover error
            self.logger.error(traceback.format_exc())

    async def listen(self):
        global channels
        while self.keep_running:
            try:
                try:
                    w = await self.queue.get()
                except RuntimeError as r_e: # pragma: no cover error
                    if "loop is closed" in str(r_e):
                        self.keep_running = False
                        return
                while self.pause_new:
                    await asyncio.sleep(0.01)
                addr = w.addr
                data = w.data
                try:
                    message = self.serializer.deserialize_message(data)
                    if self.interceptor:
                        # let test code decide to pause things before
                        # delivering
                        deliver = await self.interceptor.before_in_msg(message)
                        if not deliver:
                            self.logger.info("not delivering message %s on " \
                                             "advice from interceptor",
                                             message.code)
                            continue
                except Exception as e:  # pragma: no cover error
                    self.logger.error(traceback.format_exc())
                    self.logger.error("cannot deserialze incoming data '%s...'",
                                      data[:30])
                    continue
                self.logger.info("%s got %s from %s",
                                  self.endpoint, message.code, addr)
                # ensure addresses are tuples
                message._receiver = message.receiver[0], message.receiver[1]
                message._sender = message.sender[0], message.sender[1]
                await self.server.on_message(message)
                if self.interceptor:
                    # let test code decide to pause things after
                    # delivering
                    await self.interceptor.after_in_msg(message)
            except asyncio.exceptions.CancelledError: # pragma: no cover error
                self.keep_running = False
                return
            except GeneratorExit: # pragma: no cover error
                self.keep_running = False
                return
            except Exception as e: # pragma: no cover error
                self.logger.error(traceback.format_exc())


