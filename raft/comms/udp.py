from socket import *
import copy
import asyncio
import threading
import errno
import logging
import traceback

from ..messages.serializer import Serializer
from .comms_api import CommsAPI

class UDPComms(CommsAPI):
    
    _started = False

    async def start(self, server, endpoint):
        if self._started:
            raise Exception("can call start only once")
        self.server = server
        self.endpoint = endpoint
        self._queue = asyncio.Queue()
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(self.endpoint)
        self._logger = logging.getLogger(__name__)
        await self._start()
        thread = UDP_Server(self._sock, self)
        thread.start()
        self._logger.info('UDP Listening on %s', self.endpoint)
        self._started = True

    async def _start(self):
        udp = UDP_Protocol(
            queue=self._queue,
            message_handler=self.on_message,
            logger = self._logger,
            server=self
        )
        try:
            loop = asyncio.get_event_loop()
            self.transport, _ = await loop.create_datagram_endpoint(udp,
                                                              sock=self._sock)
            self._logger.debug("udp setup done")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            raise

    async def post_message(self, message):
        if not isinstance(message, dict):
            self._logger.debug("posting %s to %s",
                         message, message.receiver)
        await self._queue.put(message)

    async def on_message(self, data, addr):
        try:
            messages = await self.server.on_message(data, addr)
            if messages:
                for message in messages:
                    await self.post_message(messages)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            

# async class to send messages between server
class UDP_Protocol(asyncio.DatagramProtocol):
    def __init__(self, queue, message_handler, logger, server):
        self._queue = queue
        self.message_handler = message_handler
        self._server = server
        self._logger = logger
        self._logger.info('UDP_protocol created')

    def __call__(self):
        return self

    async def start(self):
        self._logger.info('UDP_protocol started')
        while not self.transport.is_closing():
            message = await self._queue.get()
            try:
                data = Serializer.serialize(message)
                self._logger.debug("sending dequed message %s (%s) to %s",
                                  message, message._type, message.receiver)
            except Exception as e:
                self._logger.error(traceback.format_exc())
                self._logger.error("error serializing queued message %s", e)
            self.transport.sendto(data, message.receiver)

    def connection_made(self, transport):
        self.transport = transport
        self._logger.info("connection made %s", transport)
        asyncio.ensure_future(self.start())

    def datagram_received(self, data, addr):
        self._logger.debug("protocol got message from %s %s", addr, data[:30])
        asyncio.ensure_future(self.message_handler(data, addr))

    def error_received(self, exc):
        self._logger.error("got error %s", exc)

    def connection_lost(self, exc):
        self._logger.info("connection lost %s", exc)

# thread to wait for message from user client
class UDP_Server(threading.Thread):
    def __init__(self, sock, server, daemon=True):
        threading.Thread.__init__(self, daemon=daemon)
        self._sock = sock
        self._server = server

    def run(self):
        while True:
            try:
                data, addr = self._sock.recvfrom(1024)
                asyncio.call_soon_threadsafe(self._server.on_message,
                                             data, addr)
            except IOError as exc:
                if exc.errno == errno.EWOULDBLOCK:
                    pass
