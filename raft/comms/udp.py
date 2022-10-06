from socket import *
import copy
import asyncio
import threading
import errno
import logging
import traceback
from collections import defaultdict

from ..messages.serializer import Serializer
from .comms_api import CommsAPI

class UDPComms(CommsAPI):
    
    _started = False

    async def start(self, server, endpoint):
        if self._started:   # pragma: no cover error
            raise Exception("can call start only once")
        self._logger = logging.getLogger(__name__)
        self.server = server
        self.endpoint = endpoint
        self._queue = asyncio.Queue()
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(self.endpoint)
        await self._start()
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
        except Exception as e: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            raise

    async def post_message(self, message):
        if not isinstance(message, dict):
            self._logger.debug("posting %s to %s",
                         message, message.receiver)
        await self._queue.put(message)

    async def on_message(self, data, addr):
        try:
            await self.server.on_message(data, addr)
        except Exception as e: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            

# async class to send messages between server
class UDP_Protocol(asyncio.DatagramProtocol):

    def __init__(self, queue, message_handler, logger, server):
        self._queue = queue
        self.message_handler = message_handler
        self._server = server
        self._logger = logger
        self._logger.info('UDP_protocol created')
        self._seq_number = -1
        self._out_of_order = defaultdict(dict)
        self._seq_by_sender = defaultdict(int)

    def __call__(self):
        return self

    async def start(self):
        self._logger.info('UDP_protocol started')
        while not self.transport.is_closing():
            message = await self._queue.get()
            self._seq_number += 1
            message.set_msg_number(self._seq_number)
            try:
                data = Serializer.serialize(message)
                self._logger.debug("sending dequed message %s (%s) to %s",
                                  message, message.code, message.receiver)
            except Exception as e:  # pragma: no cover error
                self._logger.error(traceback.format_exc())
                self._logger.error("error serializing queued message %s", e)
            self.transport.sendto(data, message.receiver)
            await asyncio.sleep(0.05)

    def connection_made(self, transport):
        self.transport = transport
        self._logger.info("connection made %s", transport)
        asyncio.ensure_future(self.start())

    def foo(self, data, addr):
        msg = Serializer.deserialize(data)
        # will never actually send a zero, always 1+
        last = self._seq_by_sender[addr]
        if msg.msg_number > last + 1:
            # defer processing
            saver = self._out_of_order[addr]
            saver[msg.msg_number] = dict(msg_number=msg.msg_number,
                                         data=data,
                                         addr=addr)
            return
        elif len(self._out_of_order[addr]):
            pending = list(self._out_of_order[addr].keys())
            pending.sort()
            first = pending[0]
            last = pending[-1]
            if msg.msg_number < first:
                if msg.msg_number == last + 1:
                    pass
            elif msg.msg_number > last:
                pass
            else:
                pass

        
    def datagram_received(self, data, addr):
        self._logger.debug("protocol got message from %s %s", addr, data[:30])
        asyncio.ensure_future(self.message_handler(data, addr))
        #await self.message_handler(data, addr)

    def error_received(self, exc):  # pragma: no cover error
        self._logger.error("got error %s", exc)

    def connection_lost(self, exc):   # pragma: no cover error
        self._logger.info("connection lost %s", exc)

