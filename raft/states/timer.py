import asyncio
import time
import logging
import traceback

class Timer:

    """Scheduling periodic callbacks"""
    def __init__(self, timer_name, term, interval, callback, source_state=None):
        self.name = timer_name
        self.term = term
        self.interval = interval
        self.callback = callback
        self.source_state = source_state
        self.task = None
        self.keep_running = False
        self.terminated = False
        self.start_time = None
        self.waiting = False
        self.logger = logging.getLogger(__name__)

    def __str__(self):
        return self.name
    
    def start(self):
        if self.terminated:
            raise Exception("tried to start on already terminated timer" \
                            f" {self.name}")
        if self.waiting:
            print("\n\ncalled start while waiting for stop on timer" \
                            f" {self.name}")
            breakpoint()
            raise Exception("called start while waiting for stop on timer" \
                            f" {self.name}")
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
            if self.source_state.server.get_state() != self.source_state:
                return
        asyncio.create_task(self.callback())
        
    async def run(self):
        while self.keep_running:
            self.start_time = time.time()
            try:
                await self.one_pass()
                if self.source_state and self.source_state.is_terminated():
                    self.logger.info("timer %s run method exiting because" \
                                     " source state is terminated",
                                     self.name)
                    break
                if (self.source_state
                    and self.source_state.server.get_state() != self.source_state):
                    self.logger.info("timer %s run method exiting because" \
                                     " source state is no longer current",
                                     self.name)
                    break
            except:
                self.logger.error(traceback.format_exc())
        if not self.keep_running:
            self.logger.info("timer %s run method exiting on stop", self.name)
        else:
            self.keep_running = False
        self.task = None
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer"  \
                            f" {self.name}")
        if not self.keep_running:
            return
        if not self.task:
            return
        self.waiting = True
        self.keep_running = False
        wait_start = time.time()
        wait_limit = self.interval + (0.1 * self.interval)
        self.logger.debug("timer %s waiting %.8f for task exit",
                          self.name, wait_limit)
        while self.task and time.time() - wait_start < wait_limit:
            await asyncio.sleep(0.001)
        self.logger.debug("timer %s done waiting for task exit", self.name)
        self.waiting = False
        if self.task:
            dur = time.time() - wait_start
            raise Exception(f"Timer {self.name} task did not exit" \
                            f" after waiting {dur:.8f}")
        
    async def reset(self):
        if self.terminated:
            raise Exception("tried to reset already terminated timer"  \
                            f" {self.name}")
        if self.waiting:
            raise Exception("called start while waiting for stop!"  \
                            f" {self.name}")
        if not self.keep_running or self.task is None:
            self.start()
        else:
            self.start_time = time.time()

    async def terminate(self):
        if self.terminated:
            raise Exception("tried to terminate already terminated timer"  \
                            f" {self.name}")
        await self.stop()
        self.terminated = True


