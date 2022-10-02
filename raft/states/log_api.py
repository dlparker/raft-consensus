import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional

@dataclass
class LogTail:
    last_index: int  = field(default = None)
    term: int  = field(default = None)
    commit_index: int  = field(default = None)

@dataclass
class LogRec:
    user_data: list =  field(default=None, repr=False)
    index: int = field(default = None)
    term: int = field(default = None)

# abstract class for all states
class Log(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_term(self) -> Union[int, None]:
        raise NotImplementedError
    
    @abc.abstractmethod
    def set_term(self, value: int):
        raise NotImplementedError
    
    @abc.abstractmethod
    def incr_term(self) -> int:
        raise NotImplementedError
    
    @abc.abstractmethod
    def get_tail(self) -> Union[LogTail, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self, index: Optional[int] = None) -> LogTail:
        raise NotImplementedError

    @abc.abstractmethod
    def get_commit_index(self) -> Union[int, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def append(self, entries: List[LogRec]) -> LogTail:
        raise NotImplementedError

    @abc.abstractmethod
    def trim_after(self, index: int) -> LogTail:
        raise NotImplementedError

    @abc.abstractmethod
    def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        raise NotImplementedError
        



        
    
