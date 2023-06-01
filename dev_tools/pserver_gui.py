#!/usr/bin/env python
import sys
import PySimpleGUI as sg
from dev_tools.monrpc import ServerTracker
import datetime

class TrackerControl:

    keep_running = False

    def attach_to_server(self, port):
        self.keep_running = True
        print(f'starting tracker for {port}')
        self.tracker = ServerTracker(8010, 'localhost', port)
        self.tracker.start()

    def pop_event(self):
        return self.tracker.pop_event()

    def stop(self):
        self.keep_running = False
        self.tracker.stop()


class App:

    def __init__(self, port):
        self.port = port
        self.tracker = TrackerControl()
        sg.theme('Dark Blue 3')  # please make your windows colorful
        
        layout = [[sg.Text(f'Tracker for server at {port}')],
                  [sg.Multiline('',
                                disabled=True,
                                size=(80,20),
                                key="TRACKER OUTPUT")],
                  [sg.Exit()]]

        self.window = sg.Window('Window that stays open', layout)

    def tracker_output(self, output):
        self.window['TRACKER OUTPUT'].print(output + "\n")
        
    def run(self):
        self.tracker.attach_to_server(self.port)
        while True:
            mon_event = self.tracker.pop_event()
            if mon_event is not None:
                dtime = datetime.datetime.now().isoformat()
                mdata = mon_event['data']
                if mon_event['event'] == "new_state":
                    self.window['TRACKER OUTPUT'].print(f'{dtime} NEW STATE! {mdata["new_state"]}')
                elif mon_event['event'] == "new_substate":
                    self.window['TRACKER OUTPUT'].print(f'{dtime} substate changed to {mdata["substate"]}')
            event, values = self.window.read(timeout=10)
            if event is sg.TIMEOUT_KEY:
                continue
            if event == sg.WIN_CLOSED or event == 'Exit':
                break
        self.tracker.stop()
        self.window.close()
        
if len(sys.argv) > 1:
    target = int(sys.argv[1])
    app = App(target)
    app.run()
else:
    raise Exception('supply port')


