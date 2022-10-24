import asyncio
import time
import logging
import traceback

from ..utils import task_logger


class Timer:

    """Scheduling periodic callbacks"""
    def __init__(self, timer_name, term, interval, callback):
        self.name = timer_name
        self.term = term
        self.interval = interval
        self.callback = callback
        self.task = None
        # Enabled flag is used by timer owner to allow or prevent
        # resets by setting and checking this state. It has no effect
        # on timer code itself
        self.enabled = False
        # Keep running is the flag used by the control methods to tell
        # the timer task when to exit. This logic around this supports
        # stopping and restarting the timer. the reset method does exactly
        # that, stops, then starts.
        self.keep_running = False
        # Flag set to indicate that no further actions will be allowed
        # with this timer. Task code will exit as soon as it sees this.
        self.terminated = False
        self.start_time = None
        # Flag indicating that there is a wait for stop in progress
        # so no other control operations will take place
        self.waiting = False
        self.logger = logging.getLogger(__name__)

    def __str__(self):
        return self.name
    
    def start(self):
        if self.terminated:
            raise Exception("tried to start on already terminated timer" \
                            f" {self.name}")
        if self.waiting:
            raise Exception("called start while waiting for stop on timer" \
                            f" {self.name}")
        self.enabled = True
        self.keep_running = True
        self.task = task_logger.create_task(self.run(),
                                            logger=self.logger,
                                            message=f"{self.name} run error")

    def disable(self):
        self.enabled = False
        self.keep_running = False
        
    def is_enabled(self):
        return self.enabled
    
    async def one_pass(self):
        while time.time() - self.start_time < self.interval:
            try:
                await asyncio.sleep(0.005)
                if not self.keep_running:
                    self.logger.info("timer %s one_pass method exiting " \
                                     "because keep_running is false",
                                     self.name)
                    return True
            except RuntimeError: # pragma: no cover error
                # someone killed the loop while we were running
                return
        task_logger.create_task(self.cb_wrapper(),
                                logger=self.logger,
                                message=f"{self.name} callback error")

    async def cb_wrapper(self):
        try:
            if self.keep_running and self.enabled and not self.terminated:
                await self.callback()
        except asyncio.exceptions.CancelledError: # pragma: no cover error 
                pass
        except:
            self.logger.error("timer %s callback raised exception\n%s",
                              self.name, traceback.format_exc())
    async def run(self):
        while self.keep_running:
            self.start_time = time.time()
            try:
                await self.one_pass()
            except asyncio.exceptions.CancelledError: # pragma: no cover error 
                pass
            except:
                self.logger.error(traceback.format_exc())
                pass
        self.logger.info("timer %s run method exiting on stop", self.name)
        self.task = None
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer"  \
                            f" {self.name}")
        if not self.keep_running or not self.task:
            return
        self.waiting = True
        self.keep_running = False
        wait_start = time.time()
        wait_limit = self.interval + (0.1 * self.interval)
        self.logger.debug("timer %s waiting %.8f for task exit",
                          self.name, wait_limit)
        while self.task and time.time() - wait_start < wait_limit:
            try:
                await asyncio.sleep(0.001) 
            except asyncio.exceptions.CancelledError: # pragma: no cover error 
                pass
        self.logger.debug("timer %s done waiting for task exit", self.name)
        self.waiting = False
        if self.task:
            dur = time.time() - wait_start
            raise Exception(f"Timer {self.name} task did not exit" \
                            f" after waiting {dur:.8f}")
        
    async def reset(self):
        if not self.enabled:
            raise Exception("tried to reset disabled timer"  \
                            f" {self.name}")
            
        if self.terminated:
            raise Exception("tried to reset already terminated timer"  \
                            f" {self.name}")
        if self.waiting:
            wait_start = time.time()
            wait_limit = self.interval + (0.1 * self.interval)
            while self.waiting and time.time() - wait_start < wait_limit:
                await asyncio.sleep(0.01)
            if self.waiting:
                dur = time.time() - wait_start
                raise Exception(f"Timer {self.name} task did not exit" \
                                f" after waiting {dur:.8f}")
            if self.terminated:
                raise Exception(f"Timer {self.name} terminated during" \
                                " call to reset")
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


