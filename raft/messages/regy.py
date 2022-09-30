from __future__ import annotations
from typing import Type
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..states.base_state import State # pragma: no cover

from .base_message import BaseMessage

@dataclass
class MsgRegyRec:
    cls: Type[BaseMessage]
    handler_name: str

class MessageRegistry:

    def __init__(self):
        self.mtypes = {}

    def register_message_class(self, message_class: Type[BaseMessage],
                               handler_name: Union[str, None]) -> None:
        code = message_class.get_code()
        if code in self.mtypes:
            old_cls = self.mtypes[code].cls
            if old_cls != message_class:
                raise Exception(f"duplicate message type names not allowed: {code}")
            return
        rec = MsgRegyRec(cls=message_class, handler_name=handler_name)
        self.mtypes[code] = rec

    def get_message_class(self, message_code) -> Type[BaseMessage]:
        rec = self.mtypes[message_code]
        return rec.cls

    def get_handler(self,  message: BaseMessage,
                    handler_object: State) -> Union[Callable[[BaseMessage,], None], None]:
        rec = self.mtypes[message.code]
        if not rec.handler_name:
            return None
        handler = getattr(handler_object, rec.handler_name)
        return handler

    def get_message_codes(self):
        return list(self.mtypes.keys())

    def get_message_classes(self):
        return [ rec.cls for rec in self.mtypes.values()]
    

regy =  MessageRegistry()

def get_message_registry():
    global regy
    return regy

def build_registry():
    from ..messages.append_entries import AppendEntriesMessage, AppendResponseMessage
    from ..messages.status import StatusQueryMessage, StatusQueryResponseMessage
    from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
    from ..messages.request_vote import RequestVoteMessage, RequestVoteResponseMessage
    from ..messages.termstart import TermStartMessage
    from ..messages.command import ClientCommandMessage, ClientCommandResultMessage
    
    global regy
    regy.register_message_class(HeartbeatMessage, "on_heartbeat")
    regy.register_message_class(HeartbeatResponseMessage, "on_heartbeat_response")
    regy.register_message_class(StatusQueryMessage, "on_status_query")
    # StatusQueries are sent by clients, so there is no handler for responses
    regy.register_message_class(StatusQueryResponseMessage, None)


    regy.register_message_class(AppendEntriesMessage, "on_append_entries")
    regy.register_message_class(AppendResponseMessage, "on_append_response")

    regy.register_message_class(RequestVoteMessage, "on_vote_request")
    regy.register_message_class(RequestVoteResponseMessage, "on_vote_received")

    #regy.register_message_class(FooMessage, "on_foo")
    #regy.register_message_class(FooResponseMessage, "on_foo_response")

    regy.register_message_class(TermStartMessage, "on_term_start")
    regy.register_message_class(ClientCommandMessage, None)
    regy.register_message_class(ClientCommandResultMessage, None)
    
build_registry()
