import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from copy import deepcopy
import logging
from .log_api import LogRec, Log

class MemoryLog(Log):

    def __init__(self):
        self.entries = []
        self.term = None
        self.commit_index = None
        self.logger = logging.getLogger(__name__)
        
    def get_term(self) -> Union[int, None]:
        return self.term
    
    def set_term(self, value: int):
        self.term = value

    def incr_term(self):
        if self.term is None:
            self.term = 0
        else:
            self.term += 1
        return self.term

    def get_commit_index(self) -> Union[int, None]:
        return self.commit_index

    def append(self, entries: List[LogRec]) -> None:
        for newitem in entries:
            save_rec = LogRec(user_data=newitem.user_data,
                              context=newitem.context)
            self.entries.append(save_rec)
            save_rec.index = len(self.entries) - 1
            if save_rec.term is None and self.term is not None:
                save_rec.term = self.term
        self.logger.debug("new log record %s", save_rec.index)

    def trim_after(self, index: int) -> None:
        if index < len(self.entries) - 1:
            self.entries = self.entries[:index + 1]
            last_rec = self.entries[index]
            self.commit_index = index
        self.logger.debug("trimmed log to %s", last_rec.index)
        
    def commit(self, index: int) -> None:
        if index > len(self.entries) - 1 or index < 0:
            raise Exception(f"cannot commit index {index}, not in records")
        self.commit_index = index
        rec = self.entries[index]
        rec.committed = True
        self.logger.debug("committed log at %s", self.commit_index)

    def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        if len(self.entries) == 0 :
            return None
        if index is None:
            index = len(self.entries) - 1 
        elif index > len(self.entries) - 1 or index < 0:
            return None
        return deepcopy(self.entries[index])

        



        
    
