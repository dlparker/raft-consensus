import copy
import asyncio
import errno
import logging
import traceback
import time

from raftframe.utils.timer import Timer
from raftframe.utils import task_logger

class Server:

    def __init__(self, live_config):
        self.live_config = live_config
        self.name = live_config.cluster.name
        self.endpoint = live_config.cluster.endpoint
        self.other_nodes = live_config.cluster.other_nodes
        self.working_dir = live_config.local.working_dir
        self.app = live_config.app
        self.log = live_config.log
        self.comms = live_config.comms
        self.state_map = live_config.state_map
        self.serializer = live_config.serializer
        self.timer_class = Timer
        self.total_nodes = len(self.other_nodes) + 1
        self.logger = logging.getLogger(__name__)
        self.comms_task = None
        self.running = False
        self.unhandled_errors = []
        self.handled_errors = []

    def start(self):
        if self.running:
            raise Exception("cannot call start twice")
        task_logger.create_task(self._start(),
                                logger=self.logger,
                                message="server start task")
        
    async def _start(self):
        self.app.set_server(self)
        self.logger.info('Server on %s activating state map', self.endpoint)
        await self.state_map.activate(self)
        self.comms_task = task_logger.create_task(
            self.comms.start(self, self.endpoint),
            logger=self.logger,
            message="server comms listener task"
        )
        self.logger.info('Server on %s', self.endpoint)
        self.running = True
        
    async def stop(self):
        if self.comms_task:
            self.comms_task.cancel()
        if self.state_map.state:
            await self.state_map.state.stop()
        self.running = False
        
    def get_serializer(self):
        return self.serializer
    
    def get_log(self):
        return self.log

    def get_app(self):
        return self.app

    def get_endpoint(self):
        return self.endpoint

    def get_state_map(self):
        return self.state_map
    
    def get_timer(self, name, term, interval, callback):
        self.logger.info("creating timer %s", name)
        return self.timer_class(name, term, interval, callback)

    def set_timer_class(self, cls):
        self.timer_class = cls

    def get_state(self):
        return self.state_map.state 

    def get_unhandled_errors(self, clear=False):
        result = self.unhandled_errors
        if clear:
            self.unhandled_errors = []
        return result

    def get_handled_errors(self, clear=False):
        result = self.handled_errors
        if clear:
            self.handled_errors = []
        return result

    def record_unexpected_state(self, state, desc):
        details = f"State {state} got {desc} \n"
        e = dict(code="state_operation_unexpected",
                 details=details)
        self.handled_errors.append(e)

    def record_failed_state_change(self, old_state, target_state,
                                   error_data):
        details = f"Change from {old_state} to {target_state} failed. "
        details += error_data
        e = dict(code="state_change_failed",
                 details=details)
        self.unhandled_errors.append(e)
        
    def record_illegal_message_state(self, sender, desc,
                                      error_data):
        details = f"Got illegal message state from {sender}, \n{desc} \n"
        details += str(error_data)
        e = dict(code="illegal_message_state",
                 details=details)
        self.unhandled_errors.append(e)

    async def on_message(self, message, recursed=False):
        try:
            pre_state = self.state_map.state
            handled = await pre_state.on_message(message)
            if not handled:
                self.logger.info("on_message handler of state %s rejected"\
                                 " message %s", pre_state, message.code)
                if pre_state == self.state_map.state:
                    start_time = time.time()
                    while (self.state_map.changing_state()
                            and time.time() - start_time < 0.2):
                        # There is a race between the instant
                        # where a state sets terminated flag
                        # and the instant at which a message might
                        # show up, so we need to wait a bit if that
                        # is happening
                        await asyncio.sleep(0.01)
                    if self.state_map.changing:
                        e = dict(code="message_rejected",
                                 message=message,
                                 details="state changing timeout")
                        self.unhandled_errors.append(e)
                        return
                if pre_state != self.state_map.state:
                    self.logger.info("changed state from %s to %s, recursing",
                                     pre_state, self.state_map.state)
                    if recursed:
                        details = "Recursed available handlers rejected message"
                        e = dict(code="message_rejected",
                                 message=message,
                                 details=details)
                        self.unhandled_errors.append(e)
                        return
                    await self.on_message(message, recursed=True)
                else:
                    e = dict(code="message_rejected",
                             message=message,
                             details="available handlers rejected message")
                    self.unhandled_errors.append(e)
        except Exception as e:  # pragma: no cover error
            self.logger.error(traceback.format_exc())
            self.logger.error("State %s got exception %s on message %s",
                              self.state_map.state, e, message)

    async def post_message(self, message):
        await self.comms.post_message(message)

    async def send_message_response(self, message):
        n = [n for n in self.other_nodes if n == message.receiver]
        if len(n) > 0:
            await self.comms.post_message(message)
        
    async def broadcast(self, message):
        for n in self.other_nodes:
            # Have to create a deep copy of message to have different receivers
            send_message = copy.deepcopy(message)
            send_message._receiver = n
            self.logger.debug("%s sending message %s to %s",
                              self.state_map.state,
                              send_message, n)
            await self.comms.post_message(send_message)
