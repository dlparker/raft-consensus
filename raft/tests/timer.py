import time
import asyncio
import threading
import logging
from collections import defaultdict

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
        

class ControlledTimer:

    def __init__(self, timer_name, interval, callback): 
        self.name = timer_name
        self.thread_id = threading.current_thread().ident
        self.eye_d = f"{self.name}_{self.thread_id}"
        self.interval = interval
        self.callback = callback
        self.logger = logging.getLogger(__name__)
        self.task = None
        self.keep_running = False
        global timer_set
        self.countdown = -1
        self.loop = None
        self.terminated = False
        if timer_set is None:
            timer_set = TimerSet()
        timer_set.register_timer(self)
        self.timer_set = timer_set

    def start(self):
        self.logger.debug("Starting timer %s", self.eye_d)
        if self.terminated:
            raise Exception("tried to start already terminated timer")
        self.keep_running = True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        self.task = asyncio.create_task(self.run())

    async def one_pass(self):
        start_time = time.time()
        while time.time() - start_time < self.interval:
            if not self.keep_running:
                return
            if self.countdown == 0:
                break
            await asyncio.sleep(0.005)
        if self.countdown == 0:
            self.keep_running = False
            self.countdown = -1
            return
        elif self.countdown > 0:
            self.countdown -= 1
        if not self.keep_running:
            return
        self.logger.debug("launching callback task from %s", self.eye_d)
        asyncio.create_task(self.callback())
        
    async def run(self):
        while self.keep_running:
            await self.one_pass()
        self.task = None
        
    async def stop(self):
        if self.terminated:
            raise Exception("tried to stop already terminated timer")
        if not self.keep_running:
            return
        self.logger.debug("Stopping timer %s", self.eye_d)
        self.keep_running = False
        self.countdown = -1
        start_time = time.time()
        while time.time() - start_time < 0.1 and self.task:
            await asyncio.sleep(0.005)
        if self.task:
            raise Exception("timer task did not exit!")
        self.logger.debug("Stopped timer %s", self.eye_d)

    async def reset(self):
        if self.terminated:
            raise Exception("tried to reset already terminated timer")
        await self.stop()
        self.start()

    async def terminate(self):
        if self.terminated:
            raise Exception("tried to stop terminate already terminated timer")
        await self.stop()
        timer_set.delete_timer(self)
        self.terminated = True
