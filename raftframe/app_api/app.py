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
        raise NotImplementedError

    @abc.abstractmethod
    async def new_substate(self, state_map: StateMap,
                            state: State,
                            substate: Substate) -> None:
        raise NotImplementedError


        
    
