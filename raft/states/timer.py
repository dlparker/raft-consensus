import asyncio
import time
import logging
import traceback

class Timer:

    """Scheduling periodic callbacks"""
    def __init__(self, timer_name, interval, callback, source_state=None):
        self.name = timer_name
        self.interval = interval
        self.callback = callback
        self.source_state = source_state
        self.task = None
        self.keep_running = False
        self.terminated = False
        self.start_time = None
        self.waiting = False
        self.logger = logging.getLogger(__name__)
        
    def start(self):
        if self.terminated:
            raise Exception("tried to start already terminated timer")
        if self.waiting:
            raise Exception("called start while waiting for stop!")
        self.keep_running = True
        self.task = asyncio.create_task(self.run())

    async def one_pass(self):
        while time.time() - self.start_time < self.interval:
            try:
                await asyncio.sleep(0.005)
            except RuntimeError:
                # someone killed the loop while we were running
                return
            if not self.keep_running:
                return
        if self.source_state:
            if self.source_state.is_terminated():
                return
        asyncio.create_task(self.callback())
        
    async def run(self):
        while self.keep_running:
            self.start_time = time.time()
            try:
                await self.one_pass()
            except:
                self.logger.error(traceback.format_exc())
        self.logger.info("timer %s run exiting", self.name)
        self.task = None
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer")
        if not self.keep_running:
            return
        if not self.task:
            return
        self.waiting = True
        self.keep_running = False
        wait_start = time.time()
        wait_time  = self.interval + (0.1 * self.interval)
        while self.task and time.time() - wait_start < wait_time:
            await asyncio.sleep(0.001)
        if self.task:
            dur = time.time() - wait_start
            raise Exception(f"Timer {self.name} task did not exit" \
                            f" after waiting {dur:.8f}")
        self.waiting = False
        
    async def reset(self):
        if self.terminated:
            raise Exception("tried to reset already terminated timer")
        if self.waiting:
            raise Exception("called start while waiting for stop!")
        if not self.keep_running or self.task is None:
            self.start()
        else:
            self.start_time = time.time()

    async def terminate(self):
        if self.terminated:
            raise Exception("tried to terminate already terminated timer")
        await self.stop()
        self.terminated = True


