import unittest
import asyncio
import time
import logging
import traceback
import os

from raftframe.tests.common_tcase import TestCaseCommon

class TestCommands(TestCaseCommon):
    
    def test_local_command(self):
        self.preamble(num_to_start=3)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_intercepts()
        self.cluster.resume_all_paused_servers()
        self.logger.debug("\n\n\tCredit 10 through direct leader call\n\n")
        self.result = None
        self.log_record_count = 0

        async def callback(result):
            self.result = result

        async def direct_command(command):
            state = self.leader.server_obj.state_map.state
            await state.on_internal_command(command, callback)
            self.log_record_count = state.log.get_last_index()

        self.loop.run_until_complete(direct_command("credit 10"))
        start_time = time.time()
        while time.time() - start_time < 2:
            if self.result is not None:
                break
            time.sleep(0.01)
        self.assertIsNotNone(self.result)
        self.assertEqual(self.result['balance'], 10)
        first_log = first.server_obj.get_log()
        self.assertEqual(first_log.get_last_index(), self.log_record_count)
        self.postamble()
