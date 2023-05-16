import unittest
import asyncio
import time
import logging
import traceback
import os
from pathlib import Path

import pytest

from raftframe.log.log_api import LogRec
from raftframe.dev_tools.memory_log import MemoryLog
from raftframe.log.sqlite_log import SqliteLog

LOGGING_TYPE=os.environ.get("TEST_LOGGING", "silent")
if LOGGING_TYPE != "silent":
    logging.root.handlers = []
    lfstring = '%(process)s %(asctime)s [%(levelname)s] %(name)s: %(message)s'
    logging.basicConfig(format=lfstring,
                        level=logging.DEBUG)

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    raft_log = logging.getLogger("raftframe")
    raft_log.setLevel(logging.DEBUG)


class TestMemoryLog(unittest.TestCase):

    def test_mem_log(self):
        mlog = MemoryLog()
        rec = mlog.read()
        self.assertIsNone(rec)
        self.assertEqual(mlog.get_last_index(), 0)
        self.assertEqual(mlog.get_last_term(), 0)
        self.assertEqual(mlog.get_term(), 0)
        mlog.incr_term()
        self.assertEqual(mlog.get_term(), 1)
        limit1 = 100
        for i in range(int(limit1/2)):
            rec = LogRec(term=1, user_data=dict(index=i))
            mlog.append([rec,])
        self.assertEqual(mlog.get_last_index(), int(limit1/2))
        self.assertEqual(mlog.get_last_term(), 1)
        
        for i in range(int(limit1/2), limit1):
            rec = LogRec(term=2, user_data=dict(index=i))
            mlog.append([rec,])
        self.assertEqual(mlog.get_last_index(), limit1)
        self.assertEqual(mlog.get_last_term(), 2)
        with self.assertRaises(Exception) as context:
            mlog.commit(111)
        with self.assertRaises(Exception) as context:
            mlog.commit(0)

        mlog.commit(1)
        self.assertEqual(mlog.get_commit_index(), 1)
        rec1 = mlog.read(1)
        self.assertTrue(rec1.committed)
        for i in range(1, limit1 + 1):
            mlog.commit(i)
        self.assertEqual(mlog.get_commit_index(), limit1)
        for i in range(2, limit1+1):
            rec = mlog.read(i)
            self.assertTrue(rec.committed)

        # now rewrite a record
        rec = mlog.read(15)
        rec.user_data = "foo"
        x = mlog.replace_or_append(rec)
        self.assertEqual(x.index, 15)
        self.assertEqual(x.user_data, "foo")
        rec.index = None
        with self.assertRaises(Exception) as context:
            y = mlog.replace_or_append(rec)
        rec.index = 0
        with self.assertRaises(Exception) as context:
            y = mlog.replace_or_append(rec)
        rec.index = limit1 + 1
        y = mlog.replace_or_append(rec)
        self.assertEqual(y.index, limit1 + 1)
        
        with self.assertRaises(Exception) as context:
            mlog.read(0)
        with self.assertRaises(Exception) as context:
            mlog.read(1000)


class TestSqliteLog(unittest.TestCase):

    def test_sqlite_log(self):
        p = Path('/tmp/log.sqlite')
        if p.exists():
            p.unlink()
        sql_log = SqliteLog("/tmp")
        rec = sql_log.read()
        self.assertIsNone(rec)
        bad = sql_log.records.read_entry(None)
        self.assertIsNone(bad)
        
        self.assertEqual(sql_log.get_last_index(), 0)
        self.assertEqual(sql_log.get_last_term(), 0)
        self.assertEqual(sql_log.get_term(), 0)
        sql_log.incr_term()
        self.assertEqual(sql_log.get_term(), 1)
        limit1 = 100
        for i in range(int(limit1/2)):
            rec = LogRec(term=1, user_data=dict(index=i))
            sql_log.append([rec,])
        self.assertEqual(sql_log.get_last_index(), int(limit1/2))
        self.assertEqual(sql_log.get_last_term(), 1)
        
        for i in range(int(limit1/2), limit1):
            rec = LogRec(term=2, user_data=dict(index=i))
            sql_log.append([rec,])
        self.assertEqual(sql_log.get_last_index(), limit1)
        self.assertEqual(sql_log.get_last_term(), 2)
        with self.assertRaises(Exception) as context:
            sql_log.commit(111)
        with self.assertRaises(Exception) as context:
            sql_log.commit(0)

        sql_log.commit(1)
        self.assertEqual(sql_log.get_commit_index(), 1)
        rec1 = sql_log.read(1)
        self.assertTrue(rec1.committed)
        for i in range(1, limit1 + 1):
            sql_log.commit(i)
        self.assertEqual(sql_log.get_commit_index(), limit1)
        for i in range(2, limit1+1):
            rec = sql_log.read(i)
            self.assertTrue(rec.committed)

        # now close the log and try some more checks, it should reopen
        sql_log.close()
        # do double close to make sure all is well
        sql_log.close()
        for i in range(2, limit1+1):
            rec = sql_log.read(i)
            self.assertTrue(rec.committed)
        
        # now rewrite a record
        rec = sql_log.read(15)
        rec.user_data = "foo"
        x = sql_log.replace_or_append(rec)
        self.assertEqual(x.index, 15)
        self.assertEqual(x.user_data, "foo")
        rec.index = None
        with self.assertRaises(Exception) as context:
            y = sql_log.replace_or_append(rec)
        rec.index = 0
        with self.assertRaises(Exception) as context:
            y = sql_log.replace_or_append(rec)
        rec.index = limit1 + 1
        y = sql_log.replace_or_append(rec)
        self.assertEqual(y.index, limit1 + 1)
        
        with self.assertRaises(Exception) as context:
            sql_log.read(0)
        with self.assertRaises(Exception) as context:
            sql_log.read(1000)
            

        # now test each operation that goes to the db, but close the db
        # before each op
        sql_log.close()
        y = sql_log.replace_or_append(rec)
        sql_log.close()
        y = sql_log.read(y.index)
        sql_log.close()
        term = sql_log.get_term()
        sql_log.close()
        last_index = sql_log.get_last_index()
        sql_log.close()
        last_term = sql_log.get_last_term()
        sql_log.close()
        commit = sql_log.get_commit_index()
        sql_log.close()
        sql_log.set_term(term + 1)

        records = sql_log.records
        records.close()
        records.save_entry(rec)
        records.close()
        records.read_entry(rec.index)
        records.close()
        records.set_term(term+2)
        

        # and make sure we get None if we read past end
        bad = records.read_entry(10000)
        self.assertIsNone(bad)
        
