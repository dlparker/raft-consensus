import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from enum import Enum

class RecordCode(str, Enum):
    """ When leader starts up, marks start of term with this """
    no_op = "NO_OP"

    """ Results of client command operation """
    client = "CLIENT" 

    """ Data used by leader to keep track of state at follower """
    follower_cursor = "follower_cursor"
    

@dataclass
class LogRec:
    code: RecordCode = field(default=RecordCode.client)
    index: int = field(default = 0)
    term: int = field(default = 0)
    committed: bool = field(default = False)
    user_data: list =  field(default=None, repr=False)
    context: dict = field(default = None, repr=False)

# abstract class for all states
class Log(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_term(self) -> int:
        raise NotImplementedError
    
    @abc.abstractmethod
    def set_term(self, value: int):
        raise NotImplementedError
    
    @abc.abstractmethod
    def incr_term(self) -> int:
        raise NotImplementedError
    
    @abc.abstractmethod
    def commit(self, index: int):
        raise NotImplementedError

    @abc.abstractmethod
    def get_commit_index(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def append(self, entries: List[LogRec]):
        raise NotImplementedError

    @abc.abstractmethod
    def replace_or_append(self, entry: LogRec) -> LogRec:
        raise NotImplementedError

    @abc.abstractmethod
    def clear_all(self):
        raise NotImplementedError

    @abc.abstractmethod
    def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_index(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_term(self) -> int:
        raise NotImplementedError

    
        



        
    
