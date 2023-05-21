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
        This is called when the StateMap is switching to a new state, before
        the StateMap reference changes, and before the new state start method
        is called, and the returned state object will be installed as the 
        new state. You should return "new_state" in almost all circumstances,
        and if not then the object you return should be a child class of the
        new_state object. Anything else will break things.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def new_substate(self, state_map: StateMap,
                            state: State,
                            substate: Substate) -> None:
        """
        This is called when the StateMap is switching to a new substate, after
        the StateMap reference changes, so the new substate is already "active"
        """
        raise NotImplementedError

    @abc.abstractmethod
    def finish_state_change(self, new_state: str) -> None:
        """
        This is called when the StateMap is switching to a new state, after 
        the StateMap reference has changed, and after the new state start method
        is called. 
        """
        raise NotImplementedError

        
    
