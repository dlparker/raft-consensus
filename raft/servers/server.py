from socket import *
import copy
import asyncio
import threading
import errno
import logging

from ..messages.serializer import Serializer
logger = logging.getLogger(__name__)

class Server(object):

    def __init__(self, name, state, log, other_nodes, endpoint, loop):
        self._name = name
        self._state = state
        self._log = log
        self.endpoint = endpoint
        self.other_nodes = other_nodes
        self._loop = loop
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
        self._lastLogIndex = 0
        self._lastLogTerm = None

        self._state.set_server(self)
        asyncio.ensure_future(self.start(), loop=self._loop)
        thread = UDP_Server(self._sock, self._loop, self)
        thread.start()
        logger.info('Listening on %s', self.endpoint)

    async def start(self):
        udp = UDP_Protocol(
            queue=self._queue,
            message_handler=self.on_message,
            loop=self._loop,
            other_nodes=self.other_nodes,
            server=self
        )
        self.transport, _ = await asyncio.Task(
            self._loop.create_datagram_endpoint(udp, sock=self._sock),
            loop=self._loop
        )

    def broadcast(self, message):
        for n in self.other_nodes:
            # Have to create a deep copy of message to have different receivers
            send_message = copy.deepcopy(message)
            send_message._receiver = n
            logger.debug("%s {self._state} sending message %s{send_message} to %s", self._state,
                   send_message, n)
            asyncio.ensure_future(self.post_message(send_message), loop=self._loop)

    def send_message_response(self, message):
        n = [n for n in self.other_nodes if n == message.receiver]
        if len(n) > 0:
            asyncio.ensure_future(self.post_message(message), loop=self._loop)

    async def post_message(self, message):
        await self._queue.put(message)

    def on_message(self, data, addr):
        addr = addr[1]
        message = None
        client_message = None
        try:
            message = Serializer.deserialize(data)
        except:
            try:
                client_message = Serializer.deserialize_client(data)
            except:
                pass

        if not message and not client_message:
            # must be an app level command
            command = data.decode('utf8')
            logger.debug("command %s", command)
            self._state.on_client_command(command, addr)
        elif message:
            message._receiver = message.receiver[0], message.receiver[1]
            message._sender = message.sender[0], message.sender[1]
            logger.debug("message %s", message)
            state_res = self._state.on_message(message)
            if state_res is None:
                logger.error("State %s cannot handle message %s",
                             self._state, message)
            else:
                state, response = state_res
                self._state = state
        elif client_message:
            self._state.on_client_command(client_message['command'], client_message['client_port'])


# async class to send messages between server
class UDP_Protocol(asyncio.DatagramProtocol):
    def __init__(self, queue, message_handler, loop, other_nodes, server):
        self._queue = queue
        self.message_handler = message_handler
        self._loop = loop
        self.other_nodes = other_nodes
        self._server = server

    def __call__(self):
        return self

    async def start(self):
        while not self.transport.is_closing():
            message = await self._queue.get()
            if not isinstance(message, dict):
                try:
                    data = Serializer.serialize(message)
                    logger.debug("sending dequed message %s to %s",
                                 message, message.receiver)
                except Exception as e:
                    logger.error("error serializing queued message %s", e)
                self.transport.sendto(data, message.receiver)
            else:
                try:
                    data = message['value'].encode('utf8')
                    addr = message['receiver']
                    logger.debug('Returning client request')
                    self._server._sock.sendto(data, addr)
                except KeyError as e:
                    if self._server._state._leaderPort is None:
                        logger.error("cannot handle client request, no leader exists")
                    else:
                        logger.info("Redirecting client request on %s %s", e, message)
                        data = Serializer.serialize_client(message['command'],
                                                           message['client_port'])
                        addr = self._server._state._leaderPort
                        self.transport.sendto(data, (addr[0], addr[1]))

    def connection_made(self, transport):
        self.transport = transport
        logger.debug("connection made %s", transport)
        asyncio.ensure_future(self.start(), loop=self._loop)

    def datagram_received(self, data, addr):
        logger.debug("protocol got message from %s %s", addr, data)
        self.message_handler(data, addr)

    def error_received(self, exc):
        logger.error("got error %s", exc)

    def connection_lost(self, exc):
        logger.error("connection lost %s")

# thread to wait for message from user client
class UDP_Server(threading.Thread):
    def __init__(self, sock, loop, server, daemon=True):
        threading.Thread.__init__(self, daemon=daemon)
        self._sock = sock
        self._loop = loop
        self._server = server

    def run(self):
        while True:
            try:
                data, addr = self._sock.recvfrom(1024)
                self._loop.call_soon_threadsafe(self._server.on_message, data, addr)
            except IOError as exc:
                if exc.errno == errno.EWOULDBLOCK:
                    pass
