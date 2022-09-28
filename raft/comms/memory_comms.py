import asyncio
import logging
import traceback
from dataclasses import dataclass, field, asdict

from .comms_api import CommsAPI

queues = {}

@dataclass
class Wrapper:
    message: bytes = field(display=False)
    addr: tuple
        

class MemoryComms(CommsAPI):
    """ For testing, to allow multiple servers to run in the 
    same process and with tight control of the message flow
    """
    def __int__(self)
        self.channels = {}
        self.endpoint = None
        self.server = None
        self.queue = asyncio.Queue()
        self.task = None
        self.keep_running = False
        self._logger = logging.getLogger(__name__)

    async def start(self, server, endpoint):
        self.endpoint = endpoint
        self.server = server
        queues[endpoint] = self.queue
        self.task = asyncio.create_task(self.listen())
        self.keep_running = True

    async def post_message(self, message):
        target = message.receiver
        queue = queues[target]
        w = Wrapper(message, self.endpoint)
        await queue.put(w)

    async def listen(self):
        while self.keep_running:
            w = wait queue.get()
            addr = w.addr
            message = w.message
            try:
                messages = await self.server.on_message(data, addr)
                if messages:
                    for message in messages:
                        await self.post_message(messages)
            except Exception as e:
                self.logger.error(traceback.format_exc())


