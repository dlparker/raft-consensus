import logging

from raft.app_api.app import StateChangeMonitor
from raft.comms.memory_comms import MessageInterceptor

from raft.tests.bt_server import MemoryBankTellerServer
from raft.tests.timer import get_timer_set

class PausingMonitor(StateChangeMonitor):

    def __init__(self, name, logger):
        self.name = name
        self.logger = logger
        self.state_map = None
        self.state_history = []
        self.substate_history = []
        self.state = None
        self.substate = None

    async def new_state(self, state_map, old_state, new_state):
        import threading
        this_id = threading.Thread.ident
        self.logger.info(f"{self.name} from {old_state} to {new_state}")
        self.state_history.append(old_state)
        self.substate_history = []
        self.state = new_state
        self.state_map = state_map
        return new_state

    async def new_substate(self, state_map, state, substate):
        import threading
        this_id = threading.Thread.ident
        self.logger.info(f"{self.name} {state} to substate {substate}")
        self.substate_history.append(self.substate)
        self.substate = substate
        self.state_map = state_map

class PausingInterceptor(MessageInterceptor):

    async def before_in_msg(self, message) -> bool:
        return True

    async def after_in_msg(self, message) -> bool:
        return True
        
    async def before_out_msg(self, message) -> bool:
        return True

    async def after_out_msg(self, message) -> bool:
        return True


class PausingBankTellerServer(MemoryBankTellerServer):

    def __init__(self, port, working_dir, name, others,
                 log_config=None, vote_at_start=True):
        super().__init__(port, working_dir, name, others,
                         log_config, vote_at_start)
        self.logger = logging.getLogger(__name__)
        self.interceptor = PausingInterceptor()
        self.comms.set_interceptor(self.interceptor)
        self.monitor = PausingMonitor(f"server_{port}", self.logger)
        self.state_map.add_state_change_monitor(self.monitor)
        self.timer_set = get_timer_set()
        
