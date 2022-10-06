import time
import asyncio
import threading
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

    def register_timer(self, timer):
        if timer.eye_d in self.recs:
            del self.recs[timer.eye_d]
        self.recs[timer.eye_d] = timer
        self.ids_by_name[timer.name].append(timer.eye_d)

    def pause_all(self):
        for timer in self.recs.values():
            timer.stop()
        
    def resume_all(self, countdown=None):
        for timer in self.recs.values():
            if countdown:
                timer.countdown = countdown
            timer.start()

    def pause_by_name(self, name):
        for tid in self.ids_by_name[name]:
            timer = self.recs[tid]
            timer.stop()
        
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
        self.task = None
        self.keep_running = False
        global timer_set
        if timer_set is None:
            timer_set = TimerSet()
        timer_set.register_timer(self)
        self.timer_set = timer_set
        self.countdown = -1
        self.loop = None

    def start(self):
        self.keep_running = True
        self.task = asyncio.create_task(self.run())

    async def one_pass(self):
        start_time = time.time()
        while time.time() - start_time < self.interval:
            await asyncio.sleep(0.1)
        await self.callback()
        
    async def run(self):
        while self.keep_running:
            await self.one_pass()
        breakpoint()
        self.task = None
                
    def stop(self):
        self.keep_running = False
        self.countdown = -1

    def reset(self):
        self.stop()
        self.start()

    def getinterval(self):
        return self.interval() if callable(self.interval) else self.interval

