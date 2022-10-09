import time
import asyncio
import threading
import logging
from collections import defaultdict

from raft.states.timer import Timer

timer_set = None

def get_timer_set():
    global timer_set
    if timer_set is None:
        timer_set = TimerSet()
    return timer_set

class TimerSet:

    def __init__(self):
        self.recs = {}
        self.ids_by_name = defaultdict(list)
        self.ids_by_thread = defaultdict(list)

    def register_timer(self, timer):
        if timer.eye_d in self.recs:
            del self.recs[timer.eye_d]
        self.recs[timer.eye_d] = timer
        self.ids_by_name[timer.name].append(timer.eye_d)
        self.ids_by_thread[threading.get_ident()].append(timer.eye_d)

    def delete_timer(self, timer):
        if timer.eye_d in self.recs:
            del self.recs[timer.eye_d]
            self.ids_by_name[timer.name].remove(timer.eye_d)
            th_id = threading.get_ident()
            self.ids_by_thread[th_id].remove(timer.eye_d)
        
    async def pause_all(self):
        for timer in self.recs.values():
            await timer.stop()
        
    def resume_all(self, countdown=None):
        for timer in self.recs.values():
            if countdown:
                timer.countdown = countdown
            timer.start()

    async def pause_by_name(self, name):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            await timer.stop()
        
    async def pause_all_this_thread(self):
        for timer_id in self.ids_by_thread[threading.get_ident()]:
            timer = self.recs[timer_id]
            await timer.stop()
        
    async def resume_all_this_thread(self):
        for timer_id in self.ids_by_thread[threading.get_ident()]:
            timer = self.recs[timer_id]
            await timer.reset()
        
    def resume_by_name(self, name, countdown=None):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            if countdown:
                timer.countdown = countdown
            timer.start()
        

class ControlledTimer(Timer):

    def __init__(self, timer_name, interval, callback, source_state=None):
        super().__init__(timer_name, interval, callback, source_state)
        self.thread_id = threading.current_thread().ident
        self.eye_d = f"{self.name}_{self.thread_id}"
        self.logger = logging.getLogger(__name__)
        self.countdown = -1
        self.terminated = False
        global timer_set
        if timer_set is None:
            timer_set = TimerSet()
        timer_set.register_timer(self)
        self.timer_set = timer_set

    def start(self):
        self.logger.debug("Starting timer %s", self.eye_d)
        super().start()

    async def one_pass(self):
        await super().one_pass()
        start_time = time.time()
        if self.countdown < 0:
            return
        if self.countdown == 0:
            self.keep_running = False
            self.countdown = -1
            return
        elif self.countdown > 0:
            self.countdown -= 1
        
    async def stop(self):
        self.logger.debug("Stopping timer %s", self.eye_d)
        await super().stop()
        self.logger.debug("Stopped timer %s", self.eye_d)

    async def reset(self):
        self.logger.debug("resetting timer %s", self.eye_d)
        await super().stop()

    async def terminate(self):
        if self.terminated:
            raise Exception("tried to stop terminate already terminated timer")
        self.logger.debug("terminating timer %s", self.eye_d)
        await super().terminate()
        timer_set.delete_timer(self)

