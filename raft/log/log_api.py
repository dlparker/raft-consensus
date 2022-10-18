import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional


@dataclass
class LogRec:
    user_data: list =  field(default=None, repr=False)
    index: int = field(default = None)
    term: int = field(default = None)
    committed: bool = field(default = False)
    context: dict = field(default = None, repr=False)

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
    def commit(self, index: int):
        raise NotImplementedError

    @abc.abstractmethod
    def get_commit_index(self) -> Union[int, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def append(self, entries: List[LogRec]):
        raise NotImplementedError

    @abc.abstractmethod
    def trim_after(self, index: int):
        raise NotImplementedError

    @abc.abstractmethod
    def clear_all(self):
        raise NotImplementedError

    @abc.abstractmethod
    def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        raise NotImplementedError

    
        



        
    
