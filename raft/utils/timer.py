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
        task_logger.create_task(self.do_start(),
                                logger=self.logger,
                                message=f"{self.name} do_start error")
        
    async def do_start(self):
        self.keep_running = True
        self.start_time = time.time()
        self.beater()
        
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

    def beater(self):
        if not self.keep_running:
            self.logger.info("timer %s beater method exiting on stop",
                             self.name)
            self.logger.info("cancelling ")
            self.timer_handle = None
            return
        if time.time() - self.start_time >= self.interval:
            self.logger.debug("timer %s calling callback", self.name)
            task_logger.create_task(self.cb_wrapper(),
                                    logger=self.logger,
                                    message=f"{self.name} callback error")
            self.start_time = time.time()
        loop = asyncio.get_running_loop()
        self.timer_handle = loop.call_later(0.005, self.beater)
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer"  \
                            f" {self.name}")
        self.keep_running = False
        if self.timer_handle:
            self.timer_handle.cancel()
            self.timer_handle = None
            
    async def reset(self):
        if self.terminated:
            raise Exception("tried to reset already terminated timer"  \
                            f" {self.name}")
        await self.do_start()
        
    async def terminate(self):
        if self.terminated:
            raise Exception("tried to terminate already terminated timer"  \
                            f" {self.name}")
        await self.stop()
        self.terminated = True


