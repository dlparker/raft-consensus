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
            for th_id, rec in self.ids_by_thread.items():
                if timer.eye_d in rec:
                    rec.remove(timer.eye_d)
        
    async def pause_all(self):
        for timer in self.recs.values():
            await timer.pause()
        
    def resume_all(self):
        for timer in self.recs.values():
            timer.start()

    async def pause_by_name(self, name):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            await timer.pause()
        
    async def pause_all_this_thread(self):
        for timer_id in self.ids_by_thread[threading.get_ident()]:
            timer = self.recs[timer_id]
            await timer.pause()
        
    async def resume_all_this_thread(self):
        for timer_id in self.ids_by_thread[threading.get_ident()]:
            timer = self.recs[timer_id]
            await timer.reset()
        
    def resume_by_name(self, name):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            timer.start()
        

class ControlledTimer(Timer):

    def __init__(self, timer_name, term, interval, callback):
        super().__init__(timer_name, term, interval, callback)
        self.thread_id = threading.current_thread().ident
        self.eye_d = f"{self.name}_{self.thread_id}"
        self.logger = logging.getLogger(__name__)
        global timer_set
        if timer_set is None:
            timer_set = TimerSet()
        timer_set.register_timer(self)
        self.timer_set = timer_set

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
        self.logger.debug("Pausing timer %s", self.eye_d)
        if self.task:
            save_task = self.task
            self.keep_running = False
            self.task.cancel()
            start_time = time.time()
            while self.task and time.time() - start_time < 1:
                await asyncio.sleep(0.01)
            if self.task:
                print(f"\n\n\t\t\t\timer {self.eye_d} would not cancel\n\n")
                raise Exception(f"timer {self.eye_d} would not cancel")
        self.logger.debug("Paused timer %s", self.eye_d)

    async def reset(self):
        self.logger.debug("resetting timer %s", self.eye_d)
        await super().reset()

    async def terminate(self):
        if self.terminated:
            raise Exception("tried to terminate already terminated timer")
        self.logger.debug("terminating timer %s", self.eye_d)
        await super().terminate()
        timer_set.delete_timer(self)

