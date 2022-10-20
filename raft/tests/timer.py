import time
import asyncio
import threading
import logging
from collections import defaultdict

from raft.states.timer import Timer

all_timer_set = None
thread_local = threading.local()

def get_timer_set():
    global all_timer_set
    global thread_timer_set
    if all_timer_set is None:
        all_timer_set = TimerSet()
    if not hasattr(thread_local, "timer_set"):
        thread_local.timer_set = TimerSet(all_timer_set)
    if thread_local.timer_set is None:
        thread_local.timer_set = TimerSet(all_timer_set)
    return all_timer_set, thread_local.timer_set

class TimerSet:

    def __init__(self, global_set=None):
        self.recs = {}
        self.logger = logging.getLogger(__name__)
        self.ids_by_name = defaultdict(list)
        self.global_set = global_set
        self.paused = False

    def register_timer(self, timer):
        if timer.eye_d in self.recs:
            self.logger.debug("deleting and re-registering %s", timer.eye_d)
            del self.recs[timer.eye_d]
        self.recs[timer.eye_d] = timer
        self.ids_by_name[timer.name].append(timer.eye_d)
        self.logger.debug("registering %s in thread %s",
                          timer.eye_d, threading.get_ident())
        if self.global_set:
            # add it to global as well
            self.global_set.register_timer(timer)
        
    def delete_timer(self, timer):
        if timer.eye_d in self.recs:
            self.logger.debug("deleting %s in thread %s",
                              timer.eye_d, threading.get_ident())
            del self.recs[timer.eye_d]
            self.ids_by_name[timer.name].remove(timer.eye_d)
        if self.global_set:
            # remove it from global as well
            self.global_set.delete_timer(timer)
        
    async def pause_all(self):
        self.paused = True
        for timer in self.recs.values():
            timer.disable()
        for timer in self.recs.values():
            await timer.pause()
        
    async def resume_all(self):
        for timer in self.recs.values():
            timer.start()
        self.paused = False

    async def pause_by_name(self, name):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            await timer.pause()
    
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
        # use the thread local timer set, let it manage the
        # global one
        self.timer_set = get_timer_set()[1]
        self.timer_set.register_timer(self)

    def start(self):
        if self.timer_set.paused:
            self.logger.warning("Not Starting timer %s, pause in set",
                                self.eye_d)
            return
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
        if self.task:
            save_task = self.task
            self.task.cancel()
            start_time = time.time()
            while self.task and time.time() - start_time < 0.01:
                try:
                    await asyncio.sleep(0.01)
                except asyncio.exceptions.CancelledError:
                    pass
            if self.task:
                print(f"\n\n\t\t\t\timer {self.eye_d} would not cancel\n\n")
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

