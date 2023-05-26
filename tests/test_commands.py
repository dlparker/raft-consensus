import unittest
import asyncio
import time
import logging
import traceback
import os

from tests.common_tcase import TestCaseCommon

class TestCommands(TestCaseCommon):
    
    def test_direct_command(self):
        self.preamble(num_to_start=3)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 through direct leader call\n\n")
        self.result = None
        self.log_record_count = 0

        async def callback(result):
            # this gets called after commit processing is successfull
            self.result = result

        async def direct_command(command):
            state = self.leader.state_map.get_state()
            await state.process_command(command, callback)
            self.log_record_count = state.log.get_last_index()

        self.loop.run_until_complete(direct_command("credit 10"))
        start_time = time.time()
        while time.time() - start_time < 2:
            if self.result is not None:
                break
            time.sleep(0.01)
        self.assertIsNotNone(self.result)
        self.assertEqual(self.result['balance'], 10)
        first_log = first.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 1:
            if first_log.get_last_index() == self.log_record_count:
                break
            time.sleep(0.01)
        self.assertEqual(first_log.get_last_index(), self.log_record_count)
        self.postamble()

    def test_remote_command(self):
        self.preamble(num_to_start=3)
        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        self.logger.debug("\n\n\tCredit 10 through client call\n\n")
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery through client \n\n")
        res = client.do_query()
        self.assertEqual(res['balance'], 10)
        self.postamble()

        
