import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from copy import deepcopy
import logging
from .log_api import LogTail, LogRec, Log

class MemoryLog(Log):

    def __init__(self):
        self._entries = []
        self._tail = LogTail()
        self.logger = logging.getLogger(__name__)
        
    def get_tail(self) -> Union[LogTail, None]:
        return deepcopy(self._tail)

    def append(self, entries: List[LogRec], term) -> LogTail:
        if term is None:
            raise Exception("term cannot be null")
        for newitem in entries:
            save_rec = LogRec(user_data=newitem.user_data)
            self._entries.append(save_rec)
            save_rec.index = len(self._entries) - 1
            save_rec.term = term
            save_rec.last_term = self._tail.term
        if not self._tail.term or term > self._tail.term:
             self._tail.term = term
        self._tail.last_index = len(self._entries) - 1
        self._tail.last_index = len(self._entries) - 1
        self.logger.debug("new log record %s", self._tail)
        return deepcopy(self._tail)

    def trim_after(self, index: int) -> LogTail:
        if index < len(self._entries) - 1:
            self._entries = self._entries[:index + 1]
            last_rec = self._entries[index]
            self._tail.last_index = index
            self._tail.term = last_rec.term
            self._tail.commit_index = index
        self.logger.debug("trimmed log to %s", self._tail)
        return deepcopy(self._tail)
        
    def commit(self, index: Optional[int] = None) -> LogTail:
        if not index:
            index = self._tail.last_index
        self._tail.commit_index = index
        self.logger.debug("committed log at %s", self._tail)
        return deepcopy(self._tail)

    def read(self, index: int) -> Union[LogRec, None]:
        if index > len(self._entries) - 1 or index < 0:
            return None
        return deepcopy(self._entries[index])

        



        
    
