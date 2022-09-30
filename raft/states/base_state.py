import logging
import asyncio
import abc
from dataclasses import asdict


from ..messages.base_message import BaseMessage
from ..messages.append_entries import AppendResponseMessage
from ..messages.status import StatusQueryResponseMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from ..messages.regy import get_message_registry


# abstract class for all server states
class State(metaclass=abc.ABCMeta):
    _type = "base"
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'on_vote_request') and 
                callable(subclass.on_vote_request) and
                hasattr(subclass, 'on_term_start') and 
                callable(subclass.on_term_start) and
                hasattr(subclass, 'on_vote_received') and 
                callable(subclass.on_vote_received) and
                hasattr(subclass, 'on_append_entries') and 
                callable(subclass.on_append_entries) and
                hasattr(subclass, 'on_append_response') and 
                callable(subclass.on_append_response) and
                hasattr(subclass, 'on_client_command') and 
                callable(subclass.on_client_command) or
                NotImplemented)
            
    def set_server(self, server):
        self._server = server

    def on_message(self, message):
        
        # If the message.term < currentTerm -> tell the sender to update term
        log = self._server.get_log()
        if (message.term and message.term > log.get_term()):
            logger = logging.getLogger(__name__)
            logger.info("updating term from %d to %s", log.get_term(),
                        message.term)
            log.set_term(message.term)

        # find the handler for the message type and call it
        regy = get_message_registry()
        handler = regy.get_handler(message, self)
        if handler:
            return handler(message)
        
    def do_heartbeat(self, message):
        return self.on_heartbeat(message)
        
    def get_type(self):
        return self._type

    def get_leader_addr(self):
        return None
    
    @abc.abstractmethod
    def on_vote_request(self, message):
        """Called when there is a vote request"""
        raise NotImplementedError

    @abc.abstractmethod
    def on_vote_received(self, message):
        """Called when this node receives a vote"""
        raise NotImplementedError

    @abc.abstractmethod
    def on_term_start(self, message):
        """Called when this node receives a term start notice from leader"""
        raise NotImplementedError

    @abc.abstractmethod
    def on_append_entries(self, message):
        """Called when there is a request for this node to append entries"""
        raise NotImplementedError

    @abc.abstractmethod
    def on_client_command(self, message, client_port):
        """Called when there is a client request"""
        raise NotImplementedError

    def on_status_query(self, message):
        """Called when there is a status query"""
        state_type = self.get_type()
        if state_type == "leader":
            leader_addr = self._server.endpoint
        elif state_type == "candidate":
            leader_addr = None
        else:
            leader_addr = self.get_leader_addr()
        log = self._server.get_log()
        log_tail = log.get_tail()
        status_data = dict(state=state_type,
                           leader=leader_addr,
                           term=log_tail.term)
        status_response = StatusQueryResponseMessage(
            self._server.endpoint,
            message.sender,
            log.get_term(),
            status_data
        )
        asyncio.ensure_future(self._server.post_message(status_response))
        return self, None

    @abc.abstractmethod
    def on_heartbeat(self, message):
        raise NotImplementedError

    def on_heartbeat_common(self, message):
        log = self._server.get_log()
        log_tail = log.get_tail()
        reply = HeartbeatResponseMessage(message.receiver,
                                         message.sender,
                                         term=log_tail.term,
                                         data=asdict(log_tail))
        self._server.post_message(reply)
        
