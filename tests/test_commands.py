import unittest
import asyncio
import time
import logging
import traceback
import os

from dev_tools.bank_app import BankingApp
from tests.common_tcase import TestCaseCommon

class SlowApp(BankingApp):

    def __init__(self, *args, **kwargs):
        self.logger = kwargs.pop('logger')
        super().__init__(*args, **kwargs)
        self.delay = None
        self.break_now = False

    async def execute_command(self, command):
        if self.delay:
            limit = self.delay * 1000
            self.logger.debug(f"\n\nExecute Command tarting delay {limit} * 0.01\n\n")
            while limit:
                await asyncio.sleep(0.01)
                limit -= 1
            self.logger.debug("\n\nExecute Command finished delay\n\n")
        if self.break_now:
            self.logger.debug("\n\nExecute Command raising exception\n\n")
            raise Exception('Ack!')
        return await super().execute_command(command)
        
    

class TestCommands(TestCaseCommon):
    
    def test_direct_command(self):
        self.preamble(num_to_start=3)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)
        self.logger.debug("\n\n\tResumed\n\n")
        self.result = None
        self.log_record_count = 0
        self.this_command = None

        async def callback(result):
            # this gets called after commit processing is successfull
            self.result = result

        async def direct_command():
            state = self.leader.state_map.get_state()
            await state.process_command(self.this_command, callback)
            self.log_record_count = state.log.get_last_index()

        self.logger.debug("\n\n\tCredit 10 through direct leader call\n\n")
        self.this_command = "credit 10"
        self.leader.add_run_in_thread_func(direct_command())
        start_time = time.time()
        while time.time() - start_time < 2:
            if self.result is not None:
                break
            time.sleep(0.01)
        self.assertIsNotNone(self.result)
        self.assertEqual(self.result.response['balance'], 10)
        first_log = first.thread.server.get_log()
        start_time = time.time()
        while time.time() - start_time < 1:
            if first_log.get_last_index() == self.log_record_count:
                break
            time.sleep(0.01)
        self.assertEqual(first_log.get_last_index(), self.log_record_count)
        self.this_command = "log_stats"
        self.result = None
        self.leader.add_run_in_thread_func(direct_command())
        start_time = time.time()
        while time.time() - start_time < 2:
            if self.result is not None:
                break
            time.sleep(0.01)
        self.assertIsNotNone(self.result)
        
        self.postamble()

    def test_delayed_command(self):
        def change_apps():
            for server in self.cluster.servers:
                server.app = SlowApp(logger=self.logger)
        self.preamble(num_to_start=3, pre_start_callback=change_apps)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)
        self.logger.debug("\n\n\tResumed\n\n")
        self.results = []
        self.log_record_count = 0
        self.this_command = None

        async def callback(result):
            # this gets called after commit processing is successfull
            #print(f"\n\n\n In Callback\n{result}\n\n")

            self.results.append(result)

        async def delay_command(command, delay):
            self.leader.app.delay = delay
            state = self.leader.state_map.get_state()
            self.logger.debug("\n\n\t%s through direct leader call\n\n", command)
            await state.process_command(command, callback)
            self.log_record_count = state.log.get_last_index()

        async def wait_for_recs(count):
            while self.log_record_count < count:
                await asyncio.sleep(0.001)
        state = self.leader.state_map.get_state()
        start_log_record_count = state.log.get_last_index()
        self.leader.add_run_in_thread_func(delay_command("credit 10", 0.1))
        self.leader.add_run_in_thread_func(delay_command("credit 5", 0.01))
        start_time = time.time()
        while len(self.results) < 2:
            time.sleep(0.001)
            if time.time() - start_time > 1.5:
                break
        self.assertEqual(self.results[0].response['balance'], 10)
        self.assertEqual(self.results[1].response['balance'], 15)
        self.postamble()

    def test_command_timeout(self):
        def change_apps():
            for server in self.cluster.servers:
                server.app = SlowApp(logger=self.logger)
        self.preamble(num_to_start=3, pre_start_callback=change_apps)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)
        self.logger.debug("\n\n\tResumed\n\n")
        self.results = []
        self.log_record_count = 0
        self.this_command = None

        async def callback(result):
            # this gets called after commit processing is successfull
            #print(f"\n\n\n In Callback\n{result}\n\n")

            self.results.append(result)

        async def delay_command(command, delay):
            self.leader.app.delay = delay
            state = self.leader.state_map.get_state()
            self.logger.debug("\n\n\t%s through direct leader call\n\n", command)
            await state.process_command(command, callback)
            self.log_record_count = state.log.get_last_index()

        async def wait_for_recs(count):
            while self.log_record_count < count:
                await asyncio.sleep(0.001)
        state = self.leader.state_map.get_state()
        state.command_timeout_limit = 0.2
        start_log_record_count = state.log.get_last_index()
        self.leader.add_run_in_thread_func(delay_command("credit 10", 0.3))
        start_time = time.time()
        while len(self.results) < 1:
            time.sleep(0.001)
            if time.time() - start_time > 1.5:
                break
        self.assertIsNotNone(self.results[0].error)
        self.postamble()

    def test_remote_command(self):
        self.preamble(num_to_start=3)
        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)
        self.logger.debug("\n\n\tCredit 10 through client call\n\n")
        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        client.do_credit(10)
        self.logger.debug("\n\n\tQuery through leader client \n\n")
        res = client.do_query()
        self.assertEqual(res['balance'], 10)

        # make command go through no-leader to make
        # sure routing through leader and back again works
        fclient = first.get_client()
        self.logger.debug("\n\n\tQuery through non-leader client \n\n")
        res = fclient.do_query()
        self.assertEqual(res['balance'], 10)
        
        self.postamble()

        
    def test_breaking_callback(self):
        self.preamble(num_to_start=3)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)
        self.logger.debug("\n\n\tResumed\n\n")
        self.result = None
        self.this_command = None
        self.break_now = False
        self.entry_count = 0

        async def callback(result):
            # this gets called after commit processing is successfull
            self.result = result
            self.logger.debug("in callback")
            self.entry_count += 1
            if self.break_now:
                raise Exception("Ack!")

        async def direct_command():
            state = self.leader.state_map.get_state()
            await state.process_command(self.this_command, callback)

        self.logger.debug("\n\n\tCredit 10 through direct leader call\n\n")
        self.break_now = True
        self.this_command = "credit 10"
        self.result = None
        self.leader.add_run_in_thread_func(direct_command())
        start_time = time.time()
        while time.time() - start_time < 2:
            if self.entry_count == 2:
                break
            time.sleep(0.01)
        self.assertIsNotNone(self.result)
        self.assertIsNotNone(self.result.error)
        self.postamble()

    def test_breaking_command_direct(self):
        def change_apps():
            for server in self.cluster.servers:
                server.app = SlowApp(logger=self.logger)
                server.app.break_now = True
        self.preamble(num_to_start=3, pre_start_callback=change_apps)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)
        self.logger.debug("\n\n\tResumed\n\n")
        self.result = None
        self.this_command = None

        async def callback(result):
            # this gets called after commit processing is successfull on on error
            self.result = result

        async def direct_command():
            state = self.leader.state_map.get_state()
            await state.process_command(self.this_command, callback)

        self.logger.debug("\n\n\tCredit 10 through direct leader call\n\n")
        self.this_command = "credit 10"
        self.leader.add_run_in_thread_func(direct_command())
        start_time = time.time()
        while time.time() - start_time < 2:
            if self.result is not None:
                break
            time.sleep(0.01)
        self.assertIsNotNone(self.result)
        self.assertIsNotNone(self.result.error)

        # Don't know how to check for the exception actually getting raised
        # and processed. All we can do is ensure next command works.
        self.postamble()

    def test_breaking_command_remote(self):
        def change_apps():
            for server in self.cluster.servers:
                server.app = SlowApp(logger=self.logger)
                server.app.break_now = True
        self.preamble(num_to_start=3, pre_start_callback=change_apps)

        first = self.non_leaders[0]
        second = self.non_leaders[1]
        self.clear_pause_triggers()
        self.cluster.resume_all()
        time.sleep(0.1)
        self.logger.debug("\n\n\tResumed\n\n")

        client = self.leader.get_client()
        status = client.get_status()
        self.assertIsNotNone(status)
        self.logger.debug("\n\n\tQuery through leader client \n\n")
        with self.assertRaises(Exception):
            res = client.do_query()

        self.postamble()
