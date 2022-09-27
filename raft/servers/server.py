from socket import *
import copy
import asyncio
import threading
import errno
import logging
import traceback

from ..messages.serializer import Serializer

class Server(object):

    def __init__(self, name, state, log, other_nodes, endpoint, loop):
        self._name = name
        self._state = state
        self._log = log
        self.endpoint = endpoint
        self.other_nodes = other_nodes
        self._queue = asyncio.Queue()
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(self.endpoint)
        self._others_ports = []
        for port in self.other_nodes:
            self._others_ports.append(port[1])

        self.client_port = None
        self._total_nodes = len(self.other_nodes) + 1
        self._commitIndex = 0
        self._currentTerm = 0
        self.logger = logging.getLogger(__name__)
        self._state.set_server(self)
        asyncio.ensure_future(self.start())
        thread = UDP_Server(self._sock, self)
        thread.start()
        # in testing, the logger config may change after file is imported, so we
        # need to get the logger again at startup
        self.logger.info('Listening on %s', self.endpoint)

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
            self.logger.error(traceback.format_exc())
            raise

    def broadcast(self, message):
        for n in self.other_nodes:
            # Have to create a deep copy of message to have different receivers
            send_message = copy.deepcopy(message)
            send_message._receiver = n
            self.logger.debug("%s sending message %s to %s", self._state,
                   send_message, n)
            asyncio.ensure_future(self.post_message(send_message))

    def send_message_response(self, message):
        n = [n for n in self.other_nodes if n == message.receiver]
        if len(n) > 0:
            asyncio.ensure_future(self.post_message(message))

    async def post_message(self, message):
        self.logger.debug("posting %s to %s",
                          message, message.receiver)
        await self._queue.put(message)

    def get_log(self):
        return self._log
    
    def on_message(self, data, addr):
        try:
            self._on_message(data, addr)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            
    def _on_message(self, data, addr):
        message = None
        try:
            message = Serializer.deserialize(data)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("cannot deserialze incoming data '%s...'",
                              data[:30])
            return
        if message.is_type("command"):
            # client does not set sender field, so set it
            message._sender = addr
            self.logger.info("command '%s' from %s", message, addr)
            if message.original_sender:
                self.logger.info("command originally from %s",
                                 message.original_sender)
                
            if self._state.get_type() == "leader":
                self._state.on_client_command(message, addr)
            else:
                leader_addr = self._state.get_leader_addr()
                if leader_addr:
                    message._receiver = leader_addr
                    message._original_sender = addr
                    self.logger.info("Redirecting client to %s on %s",
                                     leader_addr, message)
                    asyncio.ensure_future(self.post_message(message))
                else:
                    message._data = '{"error": "not available"}'
                    message._sender = message._receiver
                    message._receiver = addr
                    self.logger.info("Client getting 'unavailable', no leader")
                    asyncio.ensure_future(self.post_message(message))
        else:
            message._receiver = message.receiver[0], message.receiver[1]
            message._sender = message.sender[0], message.sender[1]
            self.logger.debug("state %s message %s", self._state, message)
            try:
                pre_state = self._state
                # TODO:
                # The code used to set the state here from the
                # response, but it never changes, as the only
                # on_message function that does this is in
                # the candidate state, and it makes the change
                # directly.
                # Probably need to clean this code up, and
                # the message dispatch code, get rid of the
                # return values as they just confuse things
                #
                # old code:
                #
                # state_res = self._state.on_message(message)
                
                self._state.on_message(message)
                if pre_state != self._state:
                    self.logger.info("changed state from %s to %s",
                                     pre_state, self._state)
                #
                # old code:
                #
                # else:
                #    state, response = state_res
                #    self._state = state
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error("State %s got exception %e on message %s",
                                  self._state, e, message)


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
