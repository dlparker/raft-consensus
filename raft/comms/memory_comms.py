import time
import asyncio
import logging
import traceback

from dataclasses import dataclass, field, asdict

from .comms_api import CommsAPI
from ..messages.serializer import Serializer

queues = {}

@dataclass
class Wrapper:
    data: bytes = field(repr=False)
    addr: tuple

class MemoryComms(CommsAPI):
    """ For testing, to allow multiple servers to run in the 
    same process and with tight control of the message flow
    """
    def __init__(self, timer_class=None):
        self.channels = {}
        self.endpoint = None
        self.server = None
        self.queue = asyncio.Queue()
        self.task = None
        self.keep_running = False
        self.timer_class = timer_class
        self.logger = logging.getLogger(__name__)
        self.in_message_holder = None
        self.out_message_holder = None

    async def start(self, server, endpoint):
        if self.timer_class:
            server.set_timer_class(self.timer_class)
        self.endpoint = endpoint
        self.server = server
        global queues
        queues[endpoint] = self.queue
        self.logger.debug("starting %s full set is %s",
                          endpoint, list(queues.keys()))
        self.task = asyncio.create_task(self.listen())
        self.keep_running = True

    async def stop(self):
        self.keep_running = False
        self.task.cancel()

    def set_in_message_holder(self, holder):
        self.in_message_holder = holder
        
    def set_out_message_holder(self, holder):
        self.out_message_holder = holder
        
    async def post_message(self, message):
        global queues
        try:
            target = message.receiver
            if target not in queues:
                start_time = time.time()
                while time.time() - start_time < 0.25:
                    await asyncio.sleep(0.001)
                    if target in queues:
                        break
                if target not in queues: # pragma: no cover error
                    self.logger.debug("%s not connected to %s",
                                      self.endpoint, target)
                    return
            queue = queues[target]
            data = Serializer.serialize(message)
            w = Wrapper(data, self.endpoint)
            self.logger.debug("%s posted %s to %s",
                              self.endpoint, message.code, target)
            if self.out_message_holder:
                await self.out_message_holder(w)
            await queue.put(w)
        except Exception: # pragma: no cover error
            self.logger.error(traceback.format_exc())

    async def listen(self):
        global queues
        while self.keep_running:
            try:
                w = await self.queue.get()
                addr = w.addr
                data = w.data
                message = Serializer.deserialize(data)
                self.logger.debug("%s got %s from %s",
                                  self.endpoint, message.code, addr)
                if self.in_message_holder:
                    await self.in_message_holder(message, addr)
                await self.server.on_message(data, addr)
            except Exception as e: # pragma: no cover error
                self.logger.error(traceback.format_exc())


