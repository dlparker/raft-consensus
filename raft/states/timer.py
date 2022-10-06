import asyncio
import time

class Timer:
    """Scheduling periodic callbacks"""
    def __init__(self, interval, callback):
        self.interval = interval
        self.callback = callback
        self.task = None
        self.keep_running = False

    def start(self):
        self.keep_running = True
        self.task = asyncio.create_task(self.run())

    async def one_pass(self):
        start_time = time.time()
        while time.time() - start_time < self.interval:
            await asyncio.sleep(0.1)
            if not self.keep_running:
                return
        await self.callback()
        
    async def run(self):
        while self.keep_running:
            await self.one_pass()
        self.task = None
        
    async def stop(self):
        self.keep_running = False
        
    async def reset(self):
        await self.stop()
        while self.task:
            await asyncio.sleep(0.001)
        self.start()

    def get_interval(self):
        return self.interval() if callable(self.interval) else self.interval

