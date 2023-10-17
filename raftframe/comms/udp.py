"""This implementation is not intended for production use. It has a
message length limitation of. It has no mechanism for dealing with
very long messages. It has insufficient error handling and testing
(see all the pragma: no cover marks for code not tested). If you want
a UDP transport for production use, then build your own, possibly by
cloning and improving this one.

"""
from socket import *
import time
import asyncio
import threading
import logging
import traceback
from collections import defaultdict

from raftframe.serializers.msgpack import MsgpackSerializer as Serializer
from raftframe.utils import task_logger
from raftframe.comms.comms_api import CommsAPI

class UDPComms(CommsAPI):
    
    started = False

    async def start(self, msg_callback, endpoint):
        if self.started:   # pragma: no cover error
            raise Exception("can call start only once")
        self.logger = logging.getLogger(__name__)
        self.msg_callback = msg_callback
        self.endpoint = endpoint
        self.transport = None
        self.out_queue = asyncio.Queue()
        self.in_queue = asyncio.Queue()
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.bind(self.endpoint)
        self.protocol = None
        await self._start()
        self.logger.info('UDP Listening on %s', self.endpoint)
        self.started = True

    async def _start(self):
        self.protocol = UDP_Protocol(
            out_queue=self.out_queue,
            in_queue=self.in_queue,
            message_handler=self.on_udp_message,
            logger = self.logger,
            server=self
        )
        try:
            loop = asyncio.get_event_loop()
            self.transport, _ = await loop.create_datagram_endpoint(self.protocol,
                                                                    sock=self.sock)
            self.logger.debug("udp setup done")
        except Exception as e: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            raise

    async def msg_listener(self):
        while self.transport:
            msg = await self.get_message()
            await self.msg_callback(msg)
            
    async def stop(self):
        if self.transport:
            self.transport.close()
            await self.out_queue.put("diediedie!")
            start_time = time.time()
            while self.protocol.running and time.time() - start_time < 1:
                await asyncio.sleep(0.001)
            if self.protocol.running: # pragma: no cover error
                raise Exception('protocol would not stop!')
            self.transport = None
            self.protocol = None
            
    def are_out_queues_empty(self):
        if self.out_queue.empty():
            return True
        return False
    
    async def post_message(self, message):
        # make sure addresses are tuples
        message._sender = (message.sender[0], message.sender[1])
        message._receiver = (message.receiver[0], message.receiver[1])
        if not isinstance(message, dict):
            self.logger.debug("posting %s to %s",
                              message, message.receiver)
        await self.out_queue.put(message)

    async def get_message(self):
        return await self.in_queue.get()
    
    async def on_udp_message(self, data, addr):
        try:
            try:
                message = Serializer.deserialize_message(data)
            except Exception as e:  # pragma: no cover error
                self.logger.error(traceback.format_exc())
                self.logger.error("cannot deserialze incoming data '%s...'",
                                  data[:30])
                return
            # ensure addresses are tuples
            message._receiver = message.receiver[0], message.receiver[1]
            message._sender = message.sender[0], message.sender[1]
            #await self.server.on_message(message)
            await self.in_queue.put(message)
        except Exception as e: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            

# async class to send messages between server
class UDP_Protocol(asyncio.DatagramProtocol):

    def __init__(self, out_queue, in_queue, message_handler, logger, server):
        self.out_queue = out_queue
        self.in_queue = in_queue
        self.message_handler = message_handler
        self.server = server
        self.logger = logger
        self.listener_task  = None
        self.running = False
        self.logger.info('UDP_protocol created')
        self.out_of_order = defaultdict(dict)
        self.seq_by_sender = defaultdict(int)
        self.seq_by_target = defaultdict(int)

    def __call__(self):
        return self

    async def start(self):
        self.running = True
        self.listener_task = task_logger.create_task(self.server.msg_listener(),
                                                     logger=self.logger,
                                                     message="starting message listener")
        self.logger.info('UDP_protocol started')
        while not self.transport.is_closing():
            try:
                message = await self.out_queue.get()
                if message == "diediedie!":
                    self.listener_task.cancel()
                    break
            except RuntimeError: # pragma: no cover error
                self.logger.info("Runtime error on queue get,"\
                                    " might be because event loop was closed")
                continue
            self.seq_by_target[message.receiver] += 1
            seq_number = self.seq_by_target[message.receiver]
            message.set_msg_number(seq_number)
            try:
                data = Serializer.serialize_message(message)
                self.logger.debug("sending dequed message %d %s (%s) to %s",
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
        self.running = False

    def connection_made(self, transport):
        self.transport = transport
        self.logger.info("connection made %s", transport)
        task_logger.create_task(self.start(),
                                logger=self.logger,
                                message="starting tranport thread")

    def datagram_received(self, data, addr):
        self.logger.debug("protocol got message from %s %s", addr, data[:30])
        task_logger.create_task(self.message_handler(data, addr),
                                logger=self.logger,
                                message=f"delivering message from {addr}")

    def error_received(self, exc):  # pragma: no cover error
        self.logger.error("got error %s", exc)

    def connection_lost(self, exc):   # pragma: no cover error
        self.logger.info("connection lost %s", exc)

