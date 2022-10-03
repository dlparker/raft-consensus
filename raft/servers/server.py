import copy
import asyncio
import errno
import logging
import traceback

from ..messages.serializer import Serializer
from ..messages.command import ClientCommandResultMessage
from ..states.timer import Timer
from ..app_api.app import App

class Server:

    def __init__(self, name, state, log, other_nodes, endpoint, comms, app):
        self._name = name
        self._log = log
        self.endpoint = endpoint
        self.other_nodes = other_nodes
        self._total_nodes = len(self.other_nodes) + 1
        self.logger = logging.getLogger(__name__)
        self.comms = comms
        self.timer_class = None
        self._state = state
        # this will only work if the state has this method,
        # currently only Follower does
        self._state.set_server(self)
        self._app = app
        self._app.set_server(self)
        self.comms_task = asyncio.create_task(self.comms.start(self, self.endpoint))
        self.logger.info('Server on %s', self.endpoint)

    def stop(self):
        self.comms_task.cancel()
        
    def get_log(self):
        return self._log

    def get_app(self):
        return self._app

    def get_endpoint(self):
        return self.endpoint
    
    def get_timer(self, name, interval, callback):
        if not self.timer_class:
            return Timer(interval, callback)
        return self.timer_class(name, interval, callback)

    def set_timer_class(self, cls):
        self.timer_class = cls

    def set_state(self, state):
        if self._state != state:
            self._state = state
    
    async def on_message(self, data, addr):
        try:
            self._on_message(data, addr)
        except Exception as e: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            
    def _on_message(self, data, addr):
        message = None
        try:
            message = Serializer.deserialize(data)
        except Exception as e:  # pragma: no cover error
            self.logger.error(traceback.format_exc())
            self.logger.error("cannot deserialze incoming data '%s...'",
                              data[:30])
            return
        message._receiver = message.receiver[0], message.receiver[1]
        message._sender = message.sender[0], message.sender[1]
        self.logger.debug("state %s message %s", self._state, message)
        try:
            pre_state = self._state
            self._state.on_message(message)
            if pre_state != self._state:
                self.logger.info("changed state from %s to %s",
                                 pre_state, self._state)
        except Exception as e:  # pragma: no cover error
            self.logger.error(traceback.format_exc())
            self.logger.error("State %s got exception %s on message %s",
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
