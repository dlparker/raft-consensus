import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from copy import deepcopy
import logging
from .log_api import LogRec, Log

class Records:

    def __init__(self):
        # log record indexes start at 1, per raft spec
        self.index = 0
        self.entries = []

    @property
    def entry_count(self):
        return len(self.entries)
    
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
        if index > self.index + 1:
            raise Exception("cannot insert record at %d, max is %d",
                            index, self.index + 1)
        self.entries[index-1] = rec
    
    def save_entry(self, rec: LogRec) -> LogRec:
        return self.insert_entry(rec)

    
class MemoryLog(Log):

    def __init__(self):
        self.records = Records()
        self.term = 0
        self.commit_index = 0
        self.logger = logging.getLogger(__name__)
        
    def get_term(self) -> Union[int, None]:
        return self.term
    
    def set_term(self, value: int):
        self.term = value

    def incr_term(self):
        self.term += 1
        return self.term

    def get_commit_index(self) -> Union[int, None]:
        return self.commit_index

    def append(self, entries: List[LogRec]) -> None:
        # make copies
        for entry in entries:
            save_rec = LogRec(code=entry.code,
                              index=None,
                              term=entry.term,
                              committed=entry.committed,
                              user_data=entry.user_data,
                              context=entry.context)
            self.records.add_entry(save_rec)
        self.logger.debug("new log record %s", save_rec.index)

    def replace_or_append(self, entry:LogRec) -> LogRec:
        if entry.index is None:
            raise Exception("api usage error, call append for new record")
        if entry.index == 0:
            raise Exception("api usage error, cannot insert at index 0")
        save_rec = LogRec(code=entry.code,
                          index=entry.index,
                          term=entry.term,
                          committed=entry.committed,
                          user_data=entry.user_data,
                          context=entry.context)
        # Normal case is that the leader will end one new record when
        # trying to get consensus, and the new record index will be
        # exactly what the next sequential record number would be.
        # If that is the case, then we just append. If not, then
        # the leader is sending us catch up records where our term
        # is not the same as the leader's term, meaning we have uncommitted
        # records from a different leader, so we overwrite the earlier
        # record by index
        if save_rec.index == self.records.index + 1:
            self.records.add_entry(save_rec)
        else:
            self.records.insert_entry(save_rec)
        return deepcopy(save_rec)
    
    def clear_all(self):
        self.entries = []
        self.commit_index = None
        
    def commit(self, index: int) -> None:
        if index < 1:
            raise Exception(f"cannot commit index {index}, not in records")
        if index > self.records.index:
            raise Exception(f"cannot commit index {index}, not in records")
        self.commit_index = index
        rec = self.records.get_entry_at(index)
        rec.committed = True
        self.records.save_entry(rec)
        self.logger.debug("committed log at %s", self.commit_index)

    def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
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

    def get_last_index(self):
        return self.records.index

    def get_last_term(self):
        if self.records.index == 0:
            return 0
        rec = self.records.get_last_entry()
        return rec.term
    



        
    
