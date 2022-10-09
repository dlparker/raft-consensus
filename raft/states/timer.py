import asyncio
import time

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
        
    def start(self):
        if self.terminated:
            raise Exception("tried to start already terminated timer")
        self.keep_running = True
        self.task = asyncio.create_task(self.run())
        self.start_time = time.time()

    async def one_pass(self):
        while time.time() - start_time < self.interval:
            await asyncio.sleep(0.005)
            if not self.keep_running:
                return
        if self.source_state:
            if self.source_state.is_terminate():
                return
        await self.callback()
        
    async def run(self):
        while self.keep_running:
            await self.one_pass()
        self.task = None
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer")
        if self.keep_running:
            return
        self.keep_running = False
        while time.time() - start_time < 0.1 and self.task:
            while self.task:
                await asyncio.sleep(0.001)
        if self.task:
            raise Exception("timer task did not exit!")
        
    async def reset(self):
        if self.terminated:
            raise Exception("tried to reset already terminated timer")
        if not self.keep_running or self.task is None:
            self.start()
        else:
            self.start_time = time.time()

    async def terminate(self):
        await self.stop()
        self.terminated = True


