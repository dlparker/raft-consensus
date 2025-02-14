import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from copy import deepcopy
import logging
from raftframe.log.log_api import LogRec, LogAPI

class Records:

    def __init__(self):
        # log record indexes start at 1, per raftframe spec
        self.index = 0
        self.entries = []

    def get_entry_at(self, index):
        if index < 1 or self.index == 0:
            return None
        return self.entries[index - 1]

    def get_last_entry(self):
        return self.get_entry_at(self.index)

    def add_entry(self, rec: LogRec) -> LogRec:
        self.index += 1
        rec.index = self.index
        self.entries.append(rec)
        return rec

    def insert_entry(self, rec: LogRec) -> LogRec:
        index = rec.index
        self.entries[index-1] = rec
    
    def save_entry(self, rec: LogRec) -> LogRec:
        return self.insert_entry(rec)

    
class MemoryLog(LogAPI):

    def __init__(self):
        self.records = Records()
        self.term = 0
        self.server = None
        self.working_directory = None
        self.logger = logging.getLogger(__name__)
        
    async def start(self, server, working_directory):
        self.server = server
        self.working_directory = working_directory
    
    async def get_term(self) -> Union[int, None]:
        if not isinstance(self.term, int):
            breakpoint()
        return self.term
    
    async def set_term(self, value: int):
        if not isinstance(value, int):
            breakpoint()
        self.term = value

    async def incr_term(self):
        self.term += 1
        return self.term

    async def append(self, entries: List[LogRec]) -> None:
        # make copies
        for entry in entries:
            save_rec = LogRec(code=entry.code,
                              index=None,
                              term=entry.term,
                              user_data=entry.user_data)
            self.records.add_entry(save_rec)
        self.logger.debug("new log record %s", save_rec.index)

    async def replace_or_append(self, entry:LogRec) -> LogRec:
        if entry.index is None:
            raise Exception("api usage error, call append for new record")
        if entry.index == 0:
            raise Exception("api usage error, cannot insert at index 0")
        save_rec = LogRec(code=entry.code,
                          index=entry.index,
                          term=entry.term,
                          user_data=entry.user_data)
        # Normal case is that the leader will end one new record when
        # it gets consensus, and the new record index will be
        # exactly what the next sequential record number would be.
        # If that is the case, then we just append. If not, then
        # probably we are in a new term and the old records don't
        # match those stored at the new leader, so it is telling
        # us to replace them. Leader is the authority, so just do it.
        if save_rec.index == self.records.index + 1:
            self.records.add_entry(save_rec)
        else:
            self.records.insert_entry(save_rec)
        return deepcopy(save_rec)
    
    async def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        if index is None:
            rec = self.records.get_last_entry()
        else:
            if index < 1:
                raise Exception(f"cannot get index {index}, not in records")
            if index > self.records.index:
                raise Exception(f"cannot get index {index}, not in records")
            rec = self.records.get_entry_at(index)
        if rec is None:
            return None
        return deepcopy(rec)

    async def get_last_index(self):
        return self.records.index

    async def get_last_term(self):
        if self.records.index == 0:
            return 0
        rec = self.records.get_last_entry()
        return rec.term
    



        
    
