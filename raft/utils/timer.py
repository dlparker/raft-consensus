import asyncio
import time
import logging
import traceback

from ..utils import task_logger


class Timer:

    """Scheduling periodic callbacks"""
    def __init__(self, timer_name, term, interval, callback):
        self.name = timer_name
        self.interval = interval
        self.callback = callback
        # Keep running is the flag used by the control methods to tell
        # the timer task when to exit. This logic around this supports
        # stopping and restarting the timer. the reset method does exactly
        # that, stops, then starts.
        self.keep_running = False
        # Flag set to indicate that no further actions will be allowed
        # with this timer. Task code will exit as soon as it sees this.
        self.terminated = False
        self.start_time = None
        self.timer_handle = None
        self.beater_interval = 0.01
        self.logger = logging.getLogger(__name__)

    def __str__(self):
        return self.name
    
    def start(self):
        if self.terminated:
            raise Exception("tried to start already terminated timer" \
                            f" {self.name}")
        if self.keep_running:
            return 
        self.keep_running = True
        self.start_time = time.time()
        loop = asyncio.get_running_loop()
        self.timer_handle = loop.call_later(self.interval, self.fire)
        
    def fire(self):
        if not self.keep_running:
            self.logger.info("timer %s fire method exiting on stop",
                             self.name)
            self.logger.info("cancelling ")
            self.timer_handle = None
            return
        task_logger.create_task(self.cb_wrapper(),
                                logger=self.logger,
                                message=f"{self.name} callback error")
        self.start_time = time.time()
        loop = asyncio.get_running_loop()
        self.timer_handle = loop.call_later(self.interval, self.fire)
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer"  \
                            f" {self.name}")
        self.keep_running = False
        if self.timer_handle:
            self.timer_handle.cancel()
            self.timer_handle = None
            
    async def cb_wrapper(self):
        try:
            if self.keep_running:
                await self.callback()
        except GeneratorExit: # pragma: no cover error
            self.keep_running = False
        except asyncio.exceptions.CancelledError: # pragma: no cover error 
                pass
        except:
            self.logger.error("timer %s callback raised exception\n%s",
                              self.name, traceback.format_exc())

    async def reset(self):
        if self.terminated:
            raise Exception("tried to reset already terminated timer"  \
                            f" {self.name}")
        if self.timer_handle:
            self.timer_handle.cancel()
            self.timer_handle = None
            self.keep_running = False
        self.start()
        
    async def terminate(self):
        if self.terminated:
            raise Exception("tried to terminate already terminated timer"  \
                            f" {self.name}")
        await self.stop()
        self.terminated = True


