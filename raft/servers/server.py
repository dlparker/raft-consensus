import copy
import asyncio
import errno
import logging
import traceback

from ..messages.serializer import Serializer
from ..messages.command import ClientCommandResultMessage

class Server:

    def __init__(self, name, state, log, other_nodes, endpoint, comms):
        self._name = name
        self._state = state
        self._log = log
        self.endpoint = endpoint
        self.other_nodes = other_nodes
        self._total_nodes = len(self.other_nodes) + 1
        self.logger = logging.getLogger(__name__)
        self._state.set_server(self)
        self.comms = comms 
        asyncio.ensure_future(self.comms.start(self, self.endpoint))
        self.logger.info('Server with UDP on %s', self.endpoint)

    def get_log(self):
        return self._log
    
    async def on_message(self, data, addr):
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
                    asyncio.ensure_future(self.comms.post_message(message))
                else:
                    response = '{"error": "not available"}'
                    client_addr = (addr[0], addr[1])
                    my_addr = (self.endpoint[0], self.endpoint[1])
                    reply = ClientCommandResultMessage(my_addr,
                                                       client_addr,
                                                       None,
                                                       response)
                    self.logger.info("Client getting 'unavailable', no leader")
                    asyncio.ensure_future(self.comms.post_message(reply))
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

    async def post_message(self, message):
        await self.comms.post_message(message)

    def send_message_response(self, message):
        n = [n for n in self.other_nodes if n == message.receiver]
        if len(n) > 0:
            asyncio.ensure_future(self.comms.post_message(message))
        
    def broadcast(self, message):
        for n in self.other_nodes:
            # Have to create a deep copy of message to have different receivers
            send_message = copy.deepcopy(message)
            send_message._receiver = n
            self.logger.debug("%s sending message %s to %s", self._state,
                   send_message, n)
            asyncio.ensure_future(self.comms.post_message(send_message))
