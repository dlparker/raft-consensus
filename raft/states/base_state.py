import logging
import asyncio
import abc
from dataclasses import asdict


from ..messages.base_message import BaseMessage
from ..messages.response import ResponseMessage
from ..messages.status import StatusQueryResponseMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage


# abstract class for all server states
class State(metaclass=abc.ABCMeta):
    _type = "base"
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'on_vote_request') and 
                callable(subclass.on_vote_request) and
                hasattr(subclass, 'on_vote_received') and 
                callable(subclass.on_vote_received) and
                hasattr(subclass, 'on_append_entries') and 
                callable(subclass.on_append_entries) and
                hasattr(subclass, 'on_response_received') and 
                callable(subclass.on_response_received) and
                hasattr(subclass, 'on_client_command') and 
                callable(subclass.on_client_command) or
                NotImplemented)
            
    def set_server(self, server):
        self._server = server

    def on_message(self, message):
        """
        Called when receiving a message, then
        calls the corresponding methods based on states
        """

        _type = message.type

        log = self._server.get_log()
        if _type == BaseMessage.StatusQuery:
            return self.on_status_query(message)
        # If the message.term < currentTerm -> tell the sender to update term
        if (message.term > log.get_term()):
            logger = logging.getLogger(__name__)
            logger.info("updating term from %d to %s", log.get_term(),
                        message.term)
            log.set_term(message.term)
        elif (message.term < log.get_term()):
            self._send_response_message(message, votedYes=False)
            return self, None
        if (_type == BaseMessage.AppendEntries):
            return self.on_append_entries(message)
        elif (_type == BaseMessage.Heartbeat):
            return self.on_heartbeat(message)
        elif (_type == BaseMessage.HeartbeatResponse):
            return self.on_heartbeat_response(message)
        elif (_type == BaseMessage.RequestVote):
            try:
                return self.on_vote_request(message)
            except NotImplementedError:
                logger = logging.getLogger(__name__)
                logger.error("can't do vote recieved %s, %s",
                             message, message.data)
        elif (_type == BaseMessage.RequestVoteResponse):
                return self.on_vote_received(message)
        elif (_type == BaseMessage.Response):
            return self.on_response_received(message)

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
    def on_append_entries(self, message):
        """Called when there is a request for this node to append entries"""
        raise NotImplementedError

    @abc.abstractmethod
    def on_response_received(self, message):
        """Called when a response is sent back to the leader"""
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
        
    def _send_response_message(self, msg, votedYes=True):
        log = self._server.get_log()
        response = ResponseMessage(
            self._server.endpoint,
            msg.sender,
            msg.term,
            {
                "response": votedYes,
                "currentTerm": log.get_term(),
            }
        )
        self._server.send_message_response(response)
