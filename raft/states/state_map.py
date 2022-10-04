import abc
from typing import Optional

from .base_state import State
from .candidate import Candidate
from .follower import Follower
from .leader import Leader

# abstract class for all states
class StateMap(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def set_server(self, server) -> None:
        """ Stores a reference to the server object. This must
        be called before any of the other methods.
        """
        raise NotImplementedError
    
    def get_server(self):
        """ Returns the reference to the server object. 
        TODO: should create a base class for Servers 
        to be used in type hints.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def activate(self) -> State:
        """ Assumes there is no current state and switches to 
        the first state, telling it and the server about each other.
        In the standard state map, the "first" state is Follower.
        StateMap implementors can choose to have some additional
        state happen prior to the switch to Follower by making
        this method switch to that one. 
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
    
    def set_server(self, server) -> None:
        self.server = server
        
    def get_server(self):
        return self.server
    
    def get_state(self) -> Optional[State]:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        return self.state

    def activate(self, server) -> State:
        self.server = server
        return self.switch_to_follower()
        
    def switch_to_follower(self, old_state: Optional[State] = None) -> Follower:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        follower =  Follower(server=self.server)
        self.server.set_state(follower)
        return follower
    
    def switch_to_candidate(self, old_state: Optional[State] = None) -> Candidate:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        candidate =  Candidate(server=self.server)
        self.server.set_state(candidate)
        return candidate

    def switch_to_leader(self, old_state: Optional[State] = None) -> Leader:
        if not self.server:
            raise Exception('must call set_server before any other method!')
        leader =  Leader(server=self.server)
        self.server.set_state(leader)
        return leader

    
    
