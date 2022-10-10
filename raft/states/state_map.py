import abc
from typing import Optional
import asyncio
import logging
import traceback

from .base_state import State
from .candidate import Candidate
from .follower import Follower
from .leader import Leader

# abstract class for all states
class StateMap(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def activate(self, server) -> State:
        """ Stores a reference to the server object. This must
        be called before any of the other methods.
        Assumes there is no current state and switches to 
        the first state, telling it and the server about each other.
        In the standard state map, the "first" state is Follower.
        StateMap implementors can choose to have some additional
        state happen prior to the switch to Follower by making
        this method switch to that one. 
        """
        raise NotImplementedError
    
    def get_server(self):
        """ Returns the reference to the server object. 
        TODO: should create a base class for Servers 
        to be used in type hints.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def get_state(self) -> State:
        raise NotImplementedError
        
    @abc.abstractmethod
    def switch_to_follower(self, old_state: Optional[State] = None) -> Follower:
        raise NotImplementedError

    @abc.abstractmethod
    def switch_to_candidate(self, old_state: Optional[State] = None) -> Candidate:
        raise NotImplementedError

    @abc.abstractmethod
    def switch_to_leader(self, old_state: Optional[State] = None) -> Leader:
        raise NotImplementedError
    
    
class StandardStateMap(StateMap):

    server = None
    state = None
    queue = None
    # can't be done with init because instance
    # of this class required for server class init
    async def activate(self, server) -> State:
        self.server = server
        self.state = None
        return await self.switch_to_follower()

    def get_server(self):
        return self.server
    
    def get_state(self) -> Optional[State]:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        return self.state

    async def switch_to_follower(self, old_state: Optional[State] = None) -> Follower:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        follower =  Follower(server=self.server)
        self.server.set_state(follower)
        return follower
    
    async def switch_to_candidate(self, old_state: Optional[State] = None) -> Candidate:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        candidate =  Candidate(server=self.server)
        self.server.set_state(candidate)
        return candidate

    async def switch_to_leader(self, old_state: Optional[State] = None) -> Leader:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        leader =  Leader(server=self.server)
        self.server.set_state(leader)
        return leader

    
    
