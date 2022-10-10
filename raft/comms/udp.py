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
        self.logger = logging.getLogger(__name__)
        self.server = server
        self.endpoint = endpoint
        self._queue = asyncio.Queue()
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(self.endpoint)
        await self._start()
        self.logger.info('UDP Listening on %s', self.endpoint)
        self._started = True

    async def _start(self):
        udp = UDP_Protocol(
            queue=self._queue,
            message_handler=self.on_message,
            logger = self.logger,
            server=self
        )
        try:
            loop = asyncio.get_event_loop()
            self.transport, _ = await loop.create_datagram_endpoint(udp,
                                                              sock=self._sock)
            self.logger.debug("udp setup done")
        except Exception as e: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            raise

    async def post_message(self, message):
        if not isinstance(message, dict):
            self.logger.debug("posting %s to %s",
                              message, message.receiver)
        await self._queue.put(message)

    async def on_message(self, data, addr):
        try:
            try:
                message = Serializer.deserialize(data)
            except Exception as e:  # pragma: no cover error
                self.logger.error(traceback.format_exc())
                self.logger.error("cannot deserialze incoming data '%s...'",
                                  data[:30])
                return
            # ensure addresses are tuples
            message._receiver = message.receiver[0], message.receiver[1]
            message._sender = message.sender[0], message.sender[1]
            await self.server.on_message(message)
        except Exception as e: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            

# async class to send messages between server
class UDP_Protocol(asyncio.DatagramProtocol):

    def __init__(self, queue, message_handler, logger, server):
        self._queue = queue
        self.message_handler = message_handler
        self._server = server
        self.logger = logger
        self.logger.info('UDP_protocol created')
        self._out_of_order = defaultdict(dict)
        self._seq_by_sender = defaultdict(int)
        self._seq_by_target = defaultdict(int)

    def __call__(self):
        return self

    async def start(self):
        self.logger.info('UDP_protocol started')
        while not self.transport.is_closing():
            message = await self._queue.get()
            self._seq_by_target[message.receiver] += 1
            seq_number = self._seq_by_target[message.receiver]
            message.set_msg_number(seq_number)
            try:
                data = Serializer.serialize(message)
                self.logger.info("sending dequed message %d %s (%s) to %s",
                                  seq_number, message,
                                  message.code, message.receiver)
            except Exception as e:  # pragma: no cover error
                self.logger.error(traceback.format_exc())
                self.logger.error("error serializing queued message %s", e)
            try:
                self.transport.sendto(data, message.receiver)
            except Exception as e:  # pragma: no cover error
                self.logger.error(traceback.format_exc())
                self.logger.error("error sending queued message %s", e)
            # git transport a chance to deliver before we dequeu another
            await asyncio.sleep(0.0001)

    def connection_made(self, transport):
        self.transport = transport
        self.logger.info("connection made %s", transport)
        asyncio.ensure_future(self.start())

    def a_datagram_received(self, data, addr):
        self.logger.debug("protocol got message from %s %s", addr, data[:30])
        msg = Serializer.deserialize(data)
        if msg.msg_number is None:
            # must be a client message
            self.logger.debug("delivery of client message")
            asyncio.ensure_future(self.message_handler(data, addr))
            return
        # will never actually send a zero, always 1+
        last = self._seq_by_sender[addr]
        if last == 0:
            # we have never gotten a message, so set to
            # allow delivery
            last = msg.msg_number - 1
        # If the message is directly after the last one
        # order is good. If the message is before the last
        # one, other process must have rebooted, so reset
        if (msg.msg_number == last + 1 or
            msg.msg_number < last):
            self.logger.info("simple delivery of msg.number %d %s",
                              msg.msg_number, msg.code)
            asyncio.ensure_future(self.message_handler(data, addr))
            self._seq_by_sender[addr] = msg.msg_number
            return
        # If the message is after the expected number, it
        # arrived out of order, so save it
        if msg.msg_number > last + 1:
            # defer processing
            saver = self._out_of_order[addr]
            saver[msg.msg_number] = dict(msg_number=msg.msg_number,
                                         data=data,
                                         addr=addr)
            self.logger.info("\n\n!! defering delivery of msg.number %d, not last %d + 1",
                              msg.msg_number, last)
            return
        # If we still haven't figured out the message,
        # see if we have pending out of order messages
        # and see if it can help us clear those, or has to be
        # added to them.
        if len(self._out_of_order[addr]) == 0:
            self.logger.error("Can't figure out ordering of message")
            breakpoint()
            return
        my_set = self._out_of_order[addr]
        pending = list(my_set.keys())
        pending.sort()
        first = pending[0]
        last = pending[-1]
        if msg.msg_number > last:
            my_set[msg.msg_number] = dict(msg_number=msg.msg_number,
                                          data=data,
                                          addr=addr)
            self.logger.info("\n\n!!! defering delivery of msg.number %d, > %d ",
                              msg.msg_number, last)
            return
        if msg.msg_number == last + 1:
            # this is the first of the late
            # arrivalsm,  handle it, then
            # work through the rest until
            # caught up or another gap
            asyncio.ensure_future(self.message_handler(data, addr))
            last = self._seq_by_sender[addr] = msg.msg_number
            for pend in pending:
                if pend != last + 1:
                    # still missing something
                    break
                rec = my_set[pend]
                asyncio.ensure_future(
                    self.message_handler(rec['data'], rec['addr']))
                del my_set[pend]
                last = self._seq_by_sender[addr] = rec['msg_number']
                self.logger.info("\n\n!!! Finished deferred delivery of msg.number %d",
                                 rec['msg_number'])
            return
        # another out of order, save it
        self.logger.info("defered delivery of msg.number %d, inside pending ",
                          msg.msg_number)
        my_set[msg.msg_number] = dict(msg_number=msg.msg_number,
                                      data=data,
                                      addr=addr)
        
    def datagram_received(self, data, addr):
        self.logger.debug("protocol got message from %s %s", addr, data[:30])
        asyncio.ensure_future(self.message_handler(data, addr))

    def error_received(self, exc):  # pragma: no cover error
        self.logger.error("got error %s", exc)

    def connection_lost(self, exc):   # pragma: no cover error
        self.logger.info("connection lost %s", exc)

