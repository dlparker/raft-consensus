from __future__ import annotations
import abc
from typing import Optional, Union
import asyncio
import logging
import traceback

from raft.app_api.app import StateChangeMonitor
from .base_state import State, Substate
from .candidate import Candidate
from .follower import Follower
from .leader import Leader


# abstract class for all states
class StateMap(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def activate(self, server) -> State:
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
    def add_state_change_monitor(self, monitor: StateChangeMonitor) -> None:
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

    def __init__(self, timeout_basis=1.0):
        self.server = None
        self.state = None
        self.queue = None
        self.logger = None
        self.monitors = None
        self.follower_leaderless_timeout = 0.75 * timeout_basis
        self.candidate_voting_timeout = 0.5 * timeout_basis
        self.leader_heartbeat_timeout = 0.5 * timeout_basis
        
    # can't be done with init because instance
    # of this class required for server class init
    async def activate(self, server) -> State:
        if not self.monitors:
            self.monitors = []
        self.server = server
        self.state = None
        self.substate = None
        self.logger = logging.getLogger(__name__)
        self.logger.info("activating state map for server %s", server)
        return await self.switch_to_follower()

    def add_state_change_monitor(self, monitor: StateChangeMonitor) -> None:
        if not self.monitors:
            self.monitors = []
        if monitor not in self.monitors:
            self.monitors.append(monitor)
            
    def get_server(self):
        return self.server
    
    def get_state(self) -> Optional[State]:
        if not self.server:
            raise Exception('must call activate before this method!')
        return self.state

    def get_substate(self) -> Optional[State]:
        if not self.server:
            raise Exception('must call activate before this method!')
        return self.substate
    
    async def set_substate(self, state, substate):
        if not self.server:
            msg = 'must call activate before this method!'
            raise Exception(msg)
        if state != self.state:
            msg = 'set_substate call on non-current state!'
            self.logger.error(msg)
            raise Exception(msg)
        for monitor in self.monitors:
            try:
                await monitor.new_substate(self, state, substate)
            except:
                self.logger.error("Monitor new_substate call got" \
                                  " exception \n\t%s",
                                  traceback.format_exc())
        self.substate = substate
        
    async def switch_to_follower(self, old_state: Optional[State] = None) -> Follower:
        if not self.server:
            raise Exception('must call activate before this method!')
        self.logger.info("switching state from %s to follower", self.state)
        follower =  Follower(server=self.server,
                             timeout=self.follower_leaderless_timeout)
        for monitor in self.monitors:
            try:
                follower = await monitor.new_state(self, self.state, follower)
            except:
                self.logger.error("Monitor new_state call got exception \n\t%s",
                                  traceback.format_exc())
        self.server.set_state(follower)
        self.state = follower
        follower.start()
        return follower
    
    async def switch_to_candidate(self, old_state: Optional[State] = None) -> Candidate:
        if not self.server:
            raise Exception('must call activate before this method!')
        self.logger.info("switching state from %s to candidate", self.state)
        candidate =  Candidate(server=self.server,
                               timeout=self.candidate_voting_timeout)
        for monitor in self.monitors:
            try:
                candidate = await monitor.new_state(self, self.state, candidate)
            except:
                self.logger.error("Monitor new_state call got exception \n%s",
                                  traceback.format_exc())
        self.server.set_state(candidate)
        self.state = candidate
        candidate.start()
        return candidate

    async def switch_to_leader(self, old_state: Optional[State] = None) -> Leader:
        if not self.server:
            raise Exception('must call activate before this method!')
        self.logger.info("switching state from %s to leader", self.state)
        leader =  Leader(server=self.server,
                         heartbeat_timeout=self.leader_heartbeat_timeout)
        for monitor in self.monitors:
            try:
                leader = await monitor.new_state(self, self.state, leader)
            except:
                self.logger.error("Monitor new_state call got exception \n%s",
                                  traceback.format_exc())
        self.server.set_state(leader)
        self.state = leader
        leader.start()
        return leader

    
    
