import time
import asyncio
import threading
import logging
from collections import defaultdict

from raft.states.timer import Timer

all_sets = []
thread_local = threading.local()

def get_timer_set():
    global thread_local
    global all_sets
    if hasattr(thread_local, "timer_set"):
        if thread_local.timer_set is not None:
            return thread_local.timer_set
    thread_local.timer_set = TimerSet()
    all_sets.append(thread_local.timer_set)
    return thread_local.timer_set

def get_all_timer_sets():
    return all_sets

class TimerSet:

    def __init__(self):
        self.recs = {}
        self.logger = logging.getLogger(__name__)
        self.ids_by_name = defaultdict(list)
        self.paused = False

    def register_timer(self, timer):
        if timer.eye_d in self.recs:
            self.logger.debug("deleting and re-registering %s", timer.eye_d)
            self.delete_timer(timer)
        self.recs[timer.eye_d] = timer
        self.ids_by_name[timer.name].append(timer.eye_d)
        self.logger.debug("registered %s in thread %s, %d total",
                          timer.eye_d, threading.get_ident(), len(self.recs))

    def delete_timer(self, timer):
        if timer.eye_d in self.recs:
            del self.recs[timer.eye_d]
            self.ids_by_name[timer.name].remove(timer.eye_d)
            self.logger.debug("deleted %s in thread %s, now %d total",
                              timer.eye_d, threading.get_ident(),
                              len(self.recs))
        
    async def pause_all(self):
        self.paused = True
        self.logger.info("Pausing %d timers in thread %s",
                         len(self.recs), threading.get_ident())
        # make a copy since pause might end up at a delete
        timers = []
        for timer in self.recs.values():
            if timer.terminated:
                continue
            timers.append(timer)
        for timer in timers:
            if timer.terminated:
                continue
            await timer.pause()
        
    def resume_all(self):
        for timer in self.recs.values():
            if timer.terminated:
                continue
            timer.start()
        self.paused = False

    async def pause_by_name(self, name):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            if timer.terminated:
                continue
            await timer.pause()
    
    def resume_by_name(self, name):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            if timer.terminated:
                continue
            timer.start()
        

class ControlledTimer(Timer):

    def __init__(self, timer_name, term, interval, callback):
        super().__init__(timer_name, term, interval, callback)
        self.thread_id = threading.current_thread().ident
        self.eye_d = f"{self.name}_{self.thread_id}"
        self.logger = logging.getLogger(__name__)
        self.timer_set = get_timer_set()
        self.timer_set.register_timer(self)

    def start(self):
        self.logger.debug("Starting timer %s", self.eye_d)
        super().start()

    async def stop(self):
        self.logger.debug("Stopping timer %s", self.eye_d)
        await super().stop()
        self.logger.debug("Stopped timer %s", self.eye_d)

    async def one_pass(self):
        try:
            await super().one_pass()
        except asyncio.exceptions.CancelledError:
            self.keep_running = False
        
    async def pause(self):
        self.logger.info("Pausing timer %s", self.eye_d)
        self.keep_running = False
        self.paused = True
        if self.task:
            start_time = time.time()
            while self.task and time.time() - start_time < 0.1:
                try:
                    await asyncio.sleep(0.01)
                except asyncio.exceptions.CancelledError:
                    pass
            if self.keep_running:
                self.logger.debug("somebody restarted timer %s while" \
                                  " wait for pause was in progress",
                                  self.eye_d)
            elif self.task:
                self.logger.error(f"\n\n\t\t\t\timer {self.eye_d} would not cancel\n\n")
                raise Exception(f"timer {self.eye_d} would not cancel")
        self.logger.info("Paused timer %s", self.eye_d)

    async def reset(self):
        self.logger.debug("resetting timer %s", self.eye_d)
        await super().reset()

    async def terminate(self):
        if self.terminated:
            raise Exception("tried to terminate already terminated timer")
        self.logger.debug("terminating timer %s", self.eye_d)
        await super().terminate()
        self.timer_set.delete_timer(self)

