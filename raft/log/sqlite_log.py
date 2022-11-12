import abc
import os
import json
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from copy import deepcopy
import logging
from raft.log.log_api import LogRec, Log, RecordCode

class Records:

    def __init__(self, storage_dir: os.PathLike):
        self.index = 0
        self.entries = []
        # log record indexes start at 1, per raft spec
        self.filepath = Path(storage_dir, "log.sqlite").resolve()
        self.db = None
        self.open()

    def open(self) -> None:
        exists = False
        if self.filepath.exists():
            exists = True
        self.db = sqlite3.connect(self.filepath,
                                  detect_types=sqlite3.PARSE_DECLTYPES |
                                  sqlite3.PARSE_COLNAMES)
        self.db.row_factory = sqlite3.Row
        self.ensure_tables()

    def ensure_tables(self):
        cursor = self.db.cursor()
        schema = f"CREATE TABLE if not exists records " \
            "(rec_index INTEGER primary key, code TEXT," \
            "term INTEGER, committed bool, " \
            "user_data TEXT, listeners TEXT) " 
        cursor.execute(schema)
        self.db.commit()
        cursor.close()
                     
    def save_entry(self, entry):
        cursor = self.db.cursor()
        params = []
        values = "("
        if entry.index is not None:
            params.append(entry.index)
            sql = f"replace into records (rec_index, "
            values += "?,"
        else:
            sql = f"insert into records ("

        sql += "code, term, committed, user_data, listeners) values "
        values += "?, ?,?,?,?)"
        sql += values
        params.append(str(entry.code.value))
        params.append(entry.term)
        params.append(entry.committed)
        params.append(json.dumps(entry.user_data))
        params.append(json.dumps(entry.listeners))
        cursor.execute(sql, params)
        entry.index = cursor.lastrowid
        self.db.commit()
        cursor.close()
        return entry

    def read_entry(self, index=None):
        cursor = self.db.cursor()
        if index == None:
            cursor.execute("select max(rec_index) from records")
            row = cursor.fetchone()
            if not row:
                cursor.close()
                return None
            index = row[0]
        sql = "select * from records where rec_index = ?"
        cursor.execute(sql, [index,])
        rec_data = cursor.fetchone()
        if rec_data is None:
            cursor.close()
            return None
        log_rec = LogRec(code=RecordCode(rec_data['code']),
                        index=rec_data['rec_index'],
                        term=rec_data['term'],
                        committed=rec_data['committed'],
                        user_data=json.loads(rec_data['user_data']),
                        listeners=json.loads(rec_data['listeners']))
        cursor.close()
        return log_rec
    
    def get_last_index(self):
        cursor = self.db.cursor()
        cursor.execute("select max(rec_index) from records")
        row = cursor.fetchone()
        if not row:
            cursor.close()
            return None
        index = row[0]
        cursor.close()
        return index
    
    def get_entry_at(self, index):
        if index < 1:
            return None
        return self.read_entry(index)

    def get_last_entry(self):
        return self.read_entry(index=None)

    def add_entry(self, rec: LogRec) -> LogRec:
        rec.index = None
        rec = self.save_entry(rec)
        return rec

    def insert_entry(self, rec: LogRec) -> LogRec:
        rec = self.save_entry(rec)
        return rec

    
class SqliteLog(Log):

    def __init__(self, storage_dir: os.PathLike):
        self.records = Records(storage_dir)
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
                              listeners=entry.listeners[::])
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
                          listeners=entry.listeners[::])
        # Normal case is that the leader will end one new record when
        # trying to get consensus, and the new record index will be
        # exactly what the next sequential record number would be.
        # If that is the case, then we just append. If not, then
        # the leader is sending us catch up records where our term
        # is not the same as the leader's term, meaning we have uncommitted
        # records from a different leader, so we overwrite the earlier
        # record by index
        if save_rec.index == self.records.get_last_index() + 1:
            self.records.add_entry(save_rec)
        else:
            self.records.insert_entry(save_rec)
        return deepcopy(save_rec)
    
    def commit(self, index: int) -> None:
        if index < 1:
            raise Exception(f"cannot commit index {index}, not in records")
        if index > self.records.get_last_index():
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
            if index > self.records.get_last_index():
                raise Exception(f"cannot get index {index}, not in records")
            rec = self.records.get_entry_at(index)
        if rec is None:
            return None
        return deepcopy(rec)

    def get_last_index(self):
        from_db = self.records.get_last_index()
        if from_db == None:
            return 0
        return from_db

    def get_last_term(self):
        if self.records.get_last_index() == None:
            return 0
        rec = self.records.get_last_entry()
        return rec.term
    



        
    
