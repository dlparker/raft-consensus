import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from .log_api import LogTail, LogRec, Log
from copy import deepcopy

class MemoryLog(Log):

    def __init__(self):
        self._entries = []
        self._tail = LogTail(-1, -1, -1, -1)
        
    def get_tail(self) -> Union[LogTail, None]:
        return self._tail

    def append(self, entries: List[LogRec], term) -> LogTail:
        for newitem in entries:
            save_rec = LogRec(user_data=newitem.user_data)
            self._entries.append(save_rec)
            save_rec.index = len(self._entries) - 1
            save_rec.term = term
            save_rec.last_term = self._tail.last_term
        self._tail.last_index = len(self._entries) - 1
        if term > self._tail.term:
            self._tail.last_term = self._tail.term
            self._tail.term = term
        self._tail.last_index = len(self._entries) - 1
        return self._tail

    def trim_after(self, index: int) -> LogTail:
        if index < len(self._entries) - 1:
            self._entries = self._entries[:index + 1]
            last_rec = self._entries[index]
            self._tail.last_index = index
            self._tail.term = last_rec.term
            self._tail.last_term = last_rec.last_term
            self._tail.commit_index = index
        return self._tail
        
    def commit(self, index: Optional[int] = None) -> LogTail:
        if not index:
            index = self._tail.last_index
        self._tail.commit_index = index
        return self._tail

    def read(self, index: int) -> Union[LogRec, None]:
        if index > len(self._entries) - 1 or index < 0:
            return None
        return deepcopy(self._entries[index])

        



        
    
