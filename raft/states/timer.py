import asyncio
import time

class Timer:
    """Scheduling periodic callbacks"""
    def __init__(self, interval, callback):
        self.interval = interval
        self.callback = callback
        self.task = None
        self.keep_running = False
        self.terminated = False

    def start(self):
        if self.terminated:
            raise Exception("tried to start already terminated timer")
        self.keep_running = True
        self.task = asyncio.create_task(self.run())

    async def one_pass(self):
        start_time = time.time()
        while time.time() - start_time < self.interval:
            await asyncio.sleep(0.005)
            if not self.keep_running:
                return
        await self.callback()
        
    async def run(self):
        while self.keep_running:
            await self.one_pass()
        self.task = None
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer")
        self.keep_running = False
        while self.task:
            await asyncio.sleep(0.001)
        
    async def reset(self):
        if self.terminated:
            raise Exception("tried to reset already terminated timer")
        await self.stop()
        self.start()

    async def terminate(self):
        await self.stop()
        self.terminated = True


