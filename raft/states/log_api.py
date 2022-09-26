import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional

@dataclass
class LogTail:
    last_index: int  = field(default = -1)
    term: int  = field(default = None)
    commit_index: int  = field(default = -1)

@dataclass
class LogRec:
    user_data: list =  field(default=None, repr=False)
    index: int = field(default = -1)
    term: int = field(default = None)
    last_term: int = field(default = -1)

# abstract class for all states
class Log(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_tail(self) -> Union[LogTail, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self, index: Optional[int] = None) -> LogTail:
        raise NotImplementedError

    @abc.abstractmethod
    def append(self, entries: List[LogRec], term) -> LogTail:
        raise NotImplementedError

    @abc.abstractmethod
    def trim_after(self, index: int) -> LogTail:
        raise NotImplementedError

    @abc.abstractmethod
    def read(self, index: int) -> Union[LogRec, None]:
        raise NotImplementedError
        



        
    
