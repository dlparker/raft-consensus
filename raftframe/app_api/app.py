from __future__ import annotations
import abc
from typing import Union
from dataclasses import dataclass, field

@dataclass
class CommandResult:
    command: str 
    response: str = field(repr=False, default=None)
    log_response: bool = field(repr=False, default=True)
    error: str = field(default=None)

# abstract class for logged interaction app code
class App(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def execute_command(self, command) -> CommandResult:
        raise NotImplementedError


# abstract class for all states
class StateChangeMonitor(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def new_state(self, state_map: StateMap,
                        old_state: Union[State, None],
                        new_state: Substate) -> State:
        """
        This is called when the StateMap is switching to a new state, after
        the StateMap reference changes but before the new state start method
        is called.

        Be carefull to ensure that your implementation does not delay for very
        long since that mess up an election and force a retry.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def new_substate(self, state_map: StateMap,
                            state: State,
                            substate: Substate) -> None:
        """
        This is called when the StateMap is switching to a new substate, after
        the StateMap reference changes, so the new substate is already "active".

        Be carefull to ensure that your implementation does not delay for very
        long since that mess up an election and force a retry.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def finish_state_change(self, new_state: str) -> None:
        """
        This is called when the StateMap is switching to a new state, after 
        the StateMap reference has changed, and after the new state start method
        is called. 
        Be carefull to ensure that your implementation does not delay for very
        long since that mess up an election and force a retry.
        """
        raise NotImplementedError

        
    
