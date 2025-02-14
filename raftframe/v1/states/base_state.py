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

class StateCode(str, Enum):

    """ A startup phase that allows app code to do things before 
    joining or after leaving cluster """
    paused = "PAUSED"

    """ Follower state, as defined in raftframe protocol """
    follower = "FOLLOWER"

    """ Candidate state, as defined in raftframe protocol """
    candidate = "CANDIDATE"

    """ Leader state, as defined in raftframe protocol """
    leader = "LEADER"
    
class Substate(str, Enum):
    """ Before any connections """
    starting = "STARTING"

    """ Follower has not received timely leader contact """
    leader_lost = "LEADER_LOST"

    """ Follower, leader has called us at least once """
    joined = "JOINED"                  

    """ Follower, as of last call from leader, log is in sync """
    synced = "SYNCED"                 

    """ Follower, leader log is different than ours """
    out_of_sync = "OUT_OF_SYNC"

    """ Last call from leader synced new records """
    synced_prepare = "SYNCED_PREPARE"

    """ Last call from leader synced commit, no new records """
    synced_commit = "SYNCED_COMMIT"

    """ Candidate starting election """
    start_election = "start_election"

    """ Broadcasting vote """
    voting = "VOTING"

    """ Just got elected """
    became_leader = "BECAME_LEADER"
    
    """ Just sent term start (as leader) """
    sent_heartbeat = "SENT_HEARTBEAT"
    
    """ Just sent new log entries append (as leader) """
    sent_new_entries = "SENT_NEW_ENTRIES"
    
    """ Just sent log entry commit (as leader) """
    sent_commit = "SENT_COMMIT"


# abstract class for all server states
class State(metaclass=abc.ABCMeta):

    def __init__(self, server, my_code):
        self.substate = Substate.starting
        self.server = server
        self.terminated = False
        self.log = server.get_log()
        self.code = my_code
    
    @classmethod
    def __subclasshook__(cls, subclass):  # pragma: no cover abstract
        return (hasattr(subclass, 'on_vote_request') and 
                callable(subclass.on_vote_request) and
                hasattr(subclass, 'on_vote_received') and 
                callable(subclass.on_vote_received) and
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
            
    def get_code(self):
        return self.code
        
    def get_term(self):
        return self.log.get_term()
    
    async def set_substate(self, substate: Substate):
        self.substate = substate
        await self.server.get_state_map().set_substate(self, substate)

    def is_terminated(self):
        return self.terminated
        
    async def on_message(self, message):
        logger = logging.getLogger(__name__)
        if self.terminated:
            # Using async for running timers, comms, etc can
            # lead to state objects executing coroutines that
            # were scheduled when the state was active, but before
            # they could run the state changed. The terminated
            # flag is set during the state change operation to
            # serve as a barrier to prevent the delayed coroutine
            # from proceeding
            logger.info("%s got message %s but already terminated, " \
                        "returning False", str(self), message.code)
            return False

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
        

    async def on_status_query(self, message):
        """Called when there is a status query"""
        if self.code == StateCode.leader:
            leader_addr = self.server.endpoint
        elif self.code == StateCode.candidate:
            leader_addr = None
        else:
            leader_addr = self.get_leader_addr()
        status_data = dict(state=self.get_code(),
                           leader=leader_addr,
                           term=self.log.get_term())
        status_response = StatusQueryResponseMessage(
            self.server.endpoint,
            message.sender,
            self.log.get_term(),
            status_data
        )
        await self.server.post_message(status_response)
        return self, None

    async def dispose_client_command(self, message, server):
        # only the leader can execute, other states should
        # call this on receipt of a client command message
        message.original_sender = message.sender
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

