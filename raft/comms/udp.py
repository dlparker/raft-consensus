from socket import *
import copy
import asyncio
import threading
import errno
import logging

from ..messages.serializer import Serializer

class UDPComms:
    def __init__(self, server, endpoint, other_nodes):
        self.server = server
        self.endpoint = endpoint
        self.other_nodes = other_nodes
        self._queue = asyncio.Queue()
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(self.endpoint)
        self.logger = logging.getLogger(__name__)
        asyncio.ensure_future(self.start())
        thread = UDP_Server(self._sock, self)
        thread.start()
        self.logger.info('UDP Listening on %s', self.endpoint)

    async def start(self):
        udp = UDP_Protocol(
            queue=self._queue,
            message_handler=self.on_message,
            other_nodes=self.other_nodes,
            server=self
        )
        try:
            loop = asyncio.get_event_loop()
            self.transport, _ = await loop.create_datagram_endpoint(udp,
                                                              sock=self._sock)
            self.logger.debug("udp setup done")
        except Exception as e:
            self.logger.error(e)
            raise

    async def post_message(self, message):
        if not isinstance(message, dict):
            self.logger.debug("posting %s to %s",
                         message, message.receiver)
        await self._queue.put(message)

    def on_message(self, data, addr):
        try:
            messages = self.server.on_message(data, addr)
            for message in messages:
                self.post_message(messages)
        except Exception as e:
            self.logger.error(e)
            

# async class to send messages between server
class UDP_Protocol(asyncio.DatagramProtocol):
    def __init__(self, queue, message_handler, other_nodes, server):
        self._queue = queue
        self.message_handler = message_handler
        self.other_nodes = other_nodes
        self._server = server
        self.logger = logging.getLogger(__name__)
        self.logger.info('UDP_protocol created')

    def __call__(self):
        return self

    async def start(self):
        self.logger.info('UDP_protocol started')
        while not self.transport.is_closing():
            message = await self._queue.get()
            try:
                data = Serializer.serialize(message)
                self.logger.debug("sending dequed message %s (%s) to %s",
                                  message, message._type, message.receiver)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error("error serializing queued message %s", e)
            self.transport.sendto(data, message.receiver)

    def connection_made(self, transport):
        self.transport = transport
        self.logger.info("connection made %s", transport)
        asyncio.ensure_future(self.start())

    def datagram_received(self, data, addr):
        self.logger.debug("protocol got message from %s %s", addr, data[:30])
        try:
            self.message_handler(data, addr)
        except Exception as e:
            self.logger.error(traceback.format_exc())

    def error_received(self, exc):
        self.logger.error("got error %s", exc)

    def connection_lost(self, exc):
        self.logger.info("connection lost %s", exc)

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
