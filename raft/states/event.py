from enum import Enum

class EventType(str, Enum):
    base = "BASE"
    timer = "TIMER"
    message = "MESSAGE"


class BaseEvent:

    def __init__(self, state_map, source_state):
        self.state_map = state_map
        self.event_type = "base"
        self.source_state = source_state

class TimerEvent(BaseEvent):

    def __init__(self, state_map, source_state, timer):
        super().__init__(state_map, source_state)
        self.timer = timer
        
class MessageEvent(BaseEvent):

    def __init__(self, state_map, source_state, message):
        super().__init__(state_map, source_state)
        self.message = message


        
        
