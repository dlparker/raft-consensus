import logging
import asyncio
import abc
from dataclasses import asdict
from enum import Enum


from ..messages.base_message import BaseMessage
from ..messages.append_entries import AppendResponseMessage
from ..messages.status import StatusQueryResponseMessage
from ..messages.heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from ..messages.command import ClientCommandResultMessage
from ..messages.regy import get_message_registry

class Substate(str, Enum):
    """ Before any connections """
    starting = "STARTING"

    """ Follower has not received timely leader contact """
    leader_lost = "LEADER_LOST"

    """ Leader has called us at least once """
    joined = "JOINED"                  

    """ As of last call from leader, log is in sync """
    synced = "SYNCED"                 

    """ Last call from leader synced commit, no new records """
    syncing_commit = "SYNCING_COMMIT"

    """ Last call from leader had new records """
    log_appending = "log_appending"

    """ Starting election """
    voting = "VOTING"

    """ Just got elected """
    became_leader = "BECAME_LEADER"
    
    """ Just sent term start (as leader) """
    sent_term_start = "SENT_TERM_START"
    
    """ Just sent term start (as leader) """
    sent_heartbeat = "SENT_HEARTBEAT"
    
    """ Just sent new log entries append (as leader) """
    sent_new_entries = "SENT_NEW_ENTRIES"
    
    """ Just sent log entrie commit (as leader) """
    sent_commit = "SENT_COMMIT"


# abstract class for all server states
class State(metaclass=abc.ABCMeta):

    def __init__(self, server, my_type):
        self._type = my_type
        self.substate = Substate.starting
        self.server = server
        self.terminated = False
    
    @classmethod
    def __subclasshook__(cls, subclass):  # pragma: no cover abstract
        return (hasattr(subclass, 'on_vote_request') and 
                callable(subclass.on_vote_request) and
                hasattr(subclass, 'on_vote_received') and 
                callable(subclass.on_vote_received) and
                hasattr(subclass, 'on_term_start') and 
                callable(subclass.on_term_start) and
                hasattr(subclass, 'on_append_entries') and 
                callable(subclass.on_append_entries) and
                hasattr(subclass, 'on_append_response') and 
                callable(subclass.on_append_response) and
                hasattr(subclass, 'on_client_command') and 
                callable(subclass.on_client_command) and
                hasattr(subclass, 'get_term') and 
                callable(subclass.get_term) and
                hasattr(subclass, 'get_leader_addr') and 
                callable(subclass.get_leader_addr) and
                hasattr(subclass, 'on_heartbeat') and 
                callable(subclass.on_heartbeat) and
                hasattr(subclass, 'on_heartbeat_response') and 
                callable(subclass.on_heartbeat_response) or
                NotImplemented)
            
        
    def get_term(self):
        log = self.server.get_log()
        return log.get_term()
    
    async def set_substate(self, substate: Substate):
        self.substate = substate
        await self.server.get_state_map().set_substate(self, substate)

    def is_terminated(self):
        return self.terminated
        
    async def on_message(self, message):
        logger = logging.getLogger(__name__)
        if self.terminated:
            logger.info("%s got message %s but already terminated, " \
                        "returning False", self._type, message.code)
            return False
        
        # If the message.term < currentTerm -> tell the sender to update term
        log = self.server.get_log()
        set_term = False
        if log.get_term() is None:
            if message.term:
                # empty log locally
                set_term = True
        if message.term is not None and  log.get_term() is not None:
            if message.term > log.get_term():
                set_term = True
        if set_term:
            logger.info("updating term from %s to %s", log.get_term(),
                        message.term)
            log.set_term(message.term)

        # find the handler for the message type and call it
        regy = get_message_registry()
        handler = regy.get_handler(message, self)
        if handler:
            res = await handler(message)
            return res
        else:
            logger.info("state %s has no handler for message %s",
                         self, message)
            return False
        
    def get_type(self):
        return self._type

    async def on_status_query(self, message):
        """Called when there is a status query"""
        state_type = self.get_type()
        if state_type == "leader":
            leader_addr = self.server.endpoint
        elif state_type == "candidate":
            leader_addr = None
        else:
            leader_addr = self.get_leader_addr()
        log = self.server.get_log()
        status_data = dict(state=state_type,
                           leader=leader_addr,
                           term=log.get_term())
        status_response = StatusQueryResponseMessage(
            self.server.endpoint,
            message.sender,
            log.get_term(),
            status_data
        )
        await self.server.post_message(status_response)
        return self, None

    async def on_heartbeat_common(self, message):
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
        else:
            # no log records yet
            last_index = -1

        data = dict(last_index=last_index)
        reply = HeartbeatResponseMessage(message.receiver,
                                         message.sender,
                                         term=log.get_term(),
                                         data=data)
        await self.server.post_message(reply)

    async def dispose_client_command(self, message, server):
        # only the leader can execute, other states should
        # call this on receipt of a client command message
        message._original_sender = message.sender
        leader_addr = self.get_leader_addr()
        if leader_addr:
            self.logger.info("%s redirecting client command %s, to leader",
                         self, message)
            message._receiver = leader_addr
            self.logger.info("Redirecting client to %s on %s",
                             leader_addr, message)
            await server.post_message(message)
        else:
            response = '{"error": "not available"}'
            reply = ClientCommandResultMessage(server.get_endpoint(),
                                               message.sender,
                                               None,
                                               response)
            self.logger.info("Client getting 'unavailable', no leader")
            await server.post_message(reply)
        
    @abc.abstractmethod
    async def on_vote_request(self, message):  # pragma: no cover abstract
        """Called when there is a vote request"""
        raise NotImplementedError

    @abc.abstractmethod
    async def on_vote_received(self, message):  # pragma: no cover abstract
        """Called when this node receives a vote"""
        raise NotImplementedError

    @abc.abstractmethod
    async def on_term_start(self, message):  # pragma: no cover abstract
        """Called when this node receives a term start notice from leader"""
        raise NotImplementedError

    @abc.abstractmethod
    async def on_append_entries(self, message):  # pragma: no cover abstract
        """Called when there is a request for this node to append entries"""
        raise NotImplementedError

    @abc.abstractmethod
    async def on_client_command(self, message):  # pragma: no cover abstract
        """Called when there is a client request"""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_leader_addr(self):  # pragma: no cover abstract
        raise NotImplementedError
    
    @abc.abstractmethod
    async def on_heartbeat(self, message): # pragma: no cover abstract
        raise NotImplementedError

    @abc.abstractmethod
    async def on_heartbeat_response(self, message): # pragma: no cover abstract
        raise NotImplementedError

