import abc
import os
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
from copy import deepcopy
import logging
from raftframe.log.log_api import LogRec, LogAPI, RecordCode
from raftframe.serializers.api import SerializerAPI

class Records:

    def __init__(self, storage_dir: os.PathLike, serializer: SerializerAPI):
        # log record indexes start at 1, per raftframe spec
        self.filepath = Path(storage_dir, "log.sqlite").resolve()
        self.serializer = serializer
        self.index = 0
        self.entries = []
        self.db = None
        self.term = -1
        self.max_commit = -1
        self.max_index = -1
        # Don't call open from here, we may be in the wrong thread,
        # at least in testing. Maybe in real server if threading is used.
        # Let it get called when the running server is trying to use it.

    def is_open(self):
        return self.db is not None
    
    def open(self) -> None:
        self.db = sqlite3.connect(self.filepath,
                                  detect_types=sqlite3.PARSE_DECLTYPES |
                                  sqlite3.PARSE_COLNAMES)
        self.db.row_factory = sqlite3.Row
        self.ensure_tables()
        cursor = self.db.cursor()
        sql = "select * from stats"
        cursor.execute(sql)
        row = cursor.fetchone()
        if row:
            self.max_index = row['max_index']
            self.max_commit = row['max_commit']
            self.term = row['term']
        else:
            self.max_index = 0
            self.max_commit = 0
            self.term = 0
            sql = "replace into stats (dummy, max_index, term, max_commit)" \
                " values (?, ?,?,?)"
            cursor.execute(sql, [1, self.max_index, self.term, self.max_commit])
        
    def close(self) -> None:
        if self.db is None:
            return
        self.db.close()
        self.db = None

    def ensure_tables(self):
        cursor = self.db.cursor()
        schema = f"CREATE TABLE if not exists records " \
            "(rec_index INTEGER primary key, code TEXT," \
            "term INTEGER, committed bool, " \
            "user_data TEXT) " 
        cursor.execute(schema)
        schema = f"CREATE TABLE if not exists stats " \
            "(dummy INTERGER primary key, max_index INTEGER," \
            " term INTEGER, max_commit INTEGER)"
        cursor.execute(schema)
        self.db.commit()
        cursor.close()
                     
    def save_entry(self, entry):
        if self.db is None:
            self.open()
        cursor = self.db.cursor()
        params = []
        values = "("
        if entry.index is not None:
            params.append(entry.index)
            sql = f"replace into records (rec_index, "
            values += "?,"
        else:
            sql = f"insert into records ("

        sql += "code, term, committed, user_data) values "
        values += "?, ?,?,?)"
        sql += values
        params.append(str(entry.code.value))
        params.append(entry.term)
        params.append(entry.committed)
        user_data = self.serializer.serialize_dict(entry.user_data)
        params.append(user_data)
        cursor.execute(sql, params)
        entry.index = cursor.lastrowid
        if cursor.lastrowid > self.max_index:
            self.max_index = cursor.lastrowid
        if entry.committed:
            if cursor.lastrowid > self.max_commit:
                self.max_commit = cursor.lastrowid
        sql = "replace into stats (dummy, max_index, term, max_commit)" \
            " values (?,?,?,?)"
        cursor.execute(sql, [1, self.max_index, self.term, self.max_commit])
        self.db.commit()
        cursor.close()
        return entry

    def read_entry(self, index=None):
        if self.db is None:
            self.open()
        cursor = self.db.cursor()
        if index == None:
            cursor.execute("select max(rec_index) from records")
            row = cursor.fetchone()
            index = row[0]
        sql = "select * from records where rec_index = ?"
        cursor.execute(sql, [index,])
        rec_data = cursor.fetchone()
        if rec_data is None:
            cursor.close()
            return None
        user_data = self.serializer.deserialize_dict(rec_data['user_data'])
        log_rec = LogRec(code=RecordCode(rec_data['code']),
                         index=rec_data['rec_index'],
                         term=rec_data['term'],
                         committed=rec_data['committed'],
                         user_data=user_data)
        cursor.close()
        return log_rec
    
    def set_term(self, value):
        if self.db is None:
            self.open()
        cursor = self.db.cursor()
        self.term = value
        sql = "replace into stats (dummy, max_index, term, max_commit)" \
            " values (?, ?,?,?)"
        cursor.execute(sql, [1, self.max_index, self.term, self.max_commit])
        self.db.commit()
        cursor.close()
        
    
    def get_entry_at(self, index):
        if index < 1:
            return None
        return self.read_entry(index)

    def add_entry(self, rec: LogRec) -> LogRec:
        rec.index = None
        rec = self.save_entry(rec)
        return rec

    def insert_entry(self, rec: LogRec) -> LogRec:
        rec = self.save_entry(rec)
        return rec

    
class SqliteLog(LogAPI):

    def __init__(self):
        self.records = None
        self.server = None
        self.working_directory = None
        self.logger = logging.getLogger(__name__)

    def start(self, server, working_directory):
        self.server = server
        self.working_directory = working_directory
        self.serializer = server.get_log_serializer()
        self.records = Records(self.working_directory, self.serializer)
        
    def close(self):
        self.records.close()
        
    def get_term(self) -> Union[int, None]:
        if not self.records.is_open():
            self.records.open()
        return self.records.term
    
    def set_term(self, value: int):
        if not self.records.is_open():
            self.records.open()
        self.records.set_term(value)

    def incr_term(self):
        if not self.records.is_open():
            self.records.open()
        self.records.set_term(self.records.term + 1)
        return self.records.term

    def get_commit_index(self) -> Union[int, None]:
        if not self.records.is_open():
            self.records.open()
        return self.records.max_commit

    def append(self, entries: List[LogRec]) -> None:
        if not self.records.is_open():
            self.records.open()
        # make copies
        for entry in entries:
            save_rec = LogRec(code=entry.code,
                              index=None,
                              term=entry.term,
                              committed=entry.committed,
                              user_data=entry.user_data)
            self.records.add_entry(save_rec)
        self.logger.debug("new log record %s", save_rec.index)

    def replace_or_append(self, entry:LogRec) -> LogRec:
        if not self.records.is_open():
            self.records.open()
        if entry.index is None:
            raise Exception("api usage error, call append for new record")
        if entry.index == 0:
            raise Exception("api usage error, cannot insert at index 0")
        save_rec = LogRec(code=entry.code,
                          index=entry.index,
                          term=entry.term,
                          committed=entry.committed,
                          user_data=entry.user_data)
        # Normal case is that the leader will end one new record when
        # trying to get consensus, and the new record index will be
        # exactly what the next sequential record number would be.
        # If that is the case, then we just append. If not, then
        # the leader is sending us catch up records where our term
        # is not the same as the leader's term, meaning we have uncommitted
        # records from a different leader, so we overwrite the earlier
        # record by index
        next_index = self.records.max_index + 1
        if save_rec.index == next_index:
            self.records.add_entry(save_rec)
        else:
            self.records.insert_entry(save_rec)
        return deepcopy(save_rec)
    
    def commit(self, index: int) -> None:
        if not self.records.is_open():
            self.records.open()
        if index < 1:
            raise Exception(f"cannot commit index {index}, not in records")
        if index > self.records.max_index:
            raise Exception(f"cannot commit index {index}, not in records")
        rec = self.records.get_entry_at(index)
        rec.committed = True
        self.records.save_entry(rec)
        self.logger.debug("committed log entry at %d, max is %d",
                          rec.index, self.records.max_commit)

    def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        if not self.records.is_open():
            self.records.open()
        if index is None:
            index = self.records.max_index
        else:
            if index < 1:
                raise Exception(f"cannot get index {index}, not in records")
            if index > self.records.max_index:
                raise Exception(f"cannot get index {index}, not in records")
        rec = self.records.get_entry_at(index)
        if rec is None:
            return None
        return deepcopy(rec)

    def get_last_index(self):
        if not self.records.is_open():
            self.records.open()
        return self.records.max_index

    def get_last_term(self):
        if not self.records.is_open():
            self.records.open()
        rec = self.records.read_entry()
        if rec is None:
            return 0
        return rec.term
    



        
    
