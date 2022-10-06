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
        self.active = False
        self.handler = None
        global timer_set
        if timer_set is None:
            timer_set = TimerSet()
        timer_set.register_timer(self)
        self.timer_set = timer_set
        self.countdown = -1
        self.loop = None

    def get_loop(self):
        if self.loop:
            return self.loop
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        return self.loop
    
    def start(self):
        self.active = True
        if not self.loop:
            self.get_loop()
        self.handler = self.loop.call_later(self.interval, self._run)

    def _run(self):
        if self.active:
            if not self.loop:
                self.get_loop()
            self.callback()
            self.handler = self.loop.call_later(self.interval, self._run)
            if self.countdown == 0:
                self.stop()
                self.countdown = -1
            elif self.countdown > 0:
                self.countdown -= 1
                
    def stop(self):
        self.active = False
        self.handler.cancel()

    def reset(self):
        self.stop()
        self.start()

    def getinterval(self):
        return self.interval() if callable(self.interval) else self.interval

