import time
import threading
import asyncio
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from raftframe.app_api.app import StateChangeMonitorAPI

PORT_OFFSET = 5000
# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class StateChangeMonitor(StateChangeMonitorAPI):

    def __init__(self, raftframe_server):
        self.raftframe_server = raftframe_server
        raftframe_server.state_map.add_state_change_monitor(self)
        self.state = None
        self.substate = None
        self.listeners = {}

    def add_listener(self, port, listener):
        self.listeners[port] = listener
        
    async def new_state(self, state_map, old_state, new_state):
        self.state = new_state
        broken = []
        for lport, listener in self.listeners.items():
            try:
                listener.new_state(self.raftframe_server.endpoint[1] + PORT_OFFSET,
                                   str(old_state),
                                   str(new_state))
            except Exception as e:
                print(f"exception on listener for port {lport}, closing")
                broken.append(lport)
        for lport in broken:
            del self.listeners[lport]
                
    async def new_substate(self, state_map, state, substate):
        if substate == self.substate:
            return
        self.substate = substate
        broken = []
        for lport, listener in self.listeners.items():
            try:
                listener.new_substate(self.raftframe_server.endpoint[1] + PORT_OFFSET,
                                      str(state),
                                      str(substate))
            except Exception as e:
                print(f"exception on listener for port {lport}, closing")
                broken.append(lport)
        for lport in broken:
            del self.listeners[lport]

    def finish_state_change(self, new_state):
        pass

class MonRpcThread(threading.Thread):

    def __init__(self, raftframe_server, port=8000):
        threading.Thread.__init__(self)
        self.raftframe_server = raftframe_server
        self.port = port
        self.server = None
        self.monitor = StateChangeMonitor(raftframe_server)

    def stop(self):
        if self.server:
            self.server.shutdown()
        
    def run(self):
        
        # Create server
        with SimpleXMLRPCServer(('localhost', self.port), logRequests=False,
                                requestHandler=RequestHandler) as server:
            self.server = server
            server.register_introspection_functions()

            @server.register_function(name="ping")
            def ping():
                return "pong"

            @server.register_function(name="get_state")
            def get_state():
                return str(self.monitor.state)
            
            @server.register_function(name="get_substate")
            def get_substate():
                return str(self.monitor.substate)
            
            @server.register_function(name="add_listener")
            def add_listener(port):
                client = xmlrpc.client.ServerProxy(f'http://localhost:{port}')
                self.monitor.add_listener(port, client)
                return "ok"
            
            # Run the server's main loop
            server.serve_forever()
        self.server = None

class RPCMonitor:

    def __init__(self, raftframe_server, port=8000):
        self.thread = MonRpcThread(raftframe_server, port)

    def start(self):
        self.thread.start()
        
    def stop(self):
        self.thread.stop()


class TrackerThread(threading.Thread):

    def __init__(self, port, server_host, server_port):
        threading.Thread.__init__(self)
        self.port = port
        self.server_host = server_host
        self.server_port = server_port
        self.server = None
        self.state = None
        self.substate = None

    def stop(self):
        if self.server:
            self.server.shutdown()
        
    def run(self):
        
        # Create server
        with SimpleXMLRPCServer(('localhost', self.port), logRequests=False,
                                requestHandler=RequestHandler) as server:
            self.server = server
            server.register_introspection_functions()

            @server.register_function(name="ping")
            def ping():
                print("got ping", flush=True)
                return "pong"

            @server.register_function(name="new_state")
            def new_state(server_port, old_state, new_state):
                self.state = new_state
                print(f"State of {server_port} changed to {new_state}")
                return "ok"
            
            @server.register_function(name="new_substate")
            def new_substate(server_port, state, substate):
                self.substate = substate
                print(f"Subtate of {server_port} ({state}) changed to {substate}")
                return "ok"
            
            # Run the server's main loop
            server.serve_forever()
        self.server = None


class ServerTracker:

    def __init__(self, port, server_host='localhost', server_port=5000):
        self.port = port
        self.server_host = server_host
        self.server_port = server_port
        self.server_rpc_port = server_port + PORT_OFFSET
        self.thread = TrackerThread(port, server_host, server_port)
        self.clients = None
        
    def start(self):
        self.thread.start()
        
        self.client =  xmlrpc.client.ServerProxy(f'http://{self.server_host}:{self.server_rpc_port}')
        self.client.add_listener(self.port)
        
    def stop(self):
        self.thread.stop()

    
        
class ClusterThread(threading.Thread):

    def __init__(self, port=8000):
        threading.Thread.__init__(self)
        self.port = port
        self.server = None
        self.servers = {}

    def stop(self):
        if self.server:
            self.server.shutdown()
        
    def run(self):
        
        # Create server
        with SimpleXMLRPCServer(('localhost', self.port), logRequests=False,
                                requestHandler=RequestHandler) as server:
            self.server = server
            server.register_introspection_functions()

            @server.register_function(name="ping")
            def ping():
                print("got ping", flush=True)
                return "pong"

            @server.register_function(name="new_state")
            def new_state(server_port, old_state, new_state):
                if str(server_port) not in self.servers:
                    self.servers[str(server_port)] = dict(state=new_state)
                else:
                    self.servers[str(server_port)]['state'] = new_state

                print(f"State of {server_port} changed to {new_state}")
                return "ok"
            
            @server.register_function(name="new_substate")
            def new_substate(server_port, state, substate):
                if str(server_port) not in self.servers:
                    self.servers[str(server_port)] = dict(state=state, substate=substate)
                else:
                    self.servers[str(server_port)]['substate'] = substate
                print(f"Subtate of {server_port} ({state}) changed to {substate}")
                return "ok"
            
            # Run the server's main loop
            server.serve_forever()
        self.server = None


class ClusterMonitor:

    def __init__(self, port=8000, server_count=3, base_port=5000):
        self.port = port
        self.thread = ClusterThread(self.port)
        self.members = [ base_port + i + PORT_OFFSET for i in range(server_count)]
        self.clients = {}
        
    def ping_all(self):
        good_count = 0
        for port, client in self.clients.items():
            try:
                client.ping()
                good_count += 1
            except:
                print(f"ping of {port} failed")
        return good_count
    
    def start(self):
        self.thread.start()
        for member in self.members:
            client =  xmlrpc.client.ServerProxy(f'http://localhost:{member}')
            self.clients[member] = client
            client.add_listener(self.port)
        
    def stop(self):
        self.thread.stop()


    
if __name__=="__main__":
    def cluster_op():
        cm = ClusterMonitor(8000)
        cm.start()
        print('entering cluster loop')
        try:
            while True:
                time.sleep(1)
                good_count = cm.ping_all()
                if good_count == 0:
                    print("no good clients, quitting")
                    break
        except KeyboardInterrupt:
            pass
        finally:
            cm.stop()
    
    def server_op(port):
        print(f'starting tracker for {port}')
        st = ServerTracker(8010, 'localhost', port)
        st.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            st.stop()
            
    import sys
    if len(sys.argv) > 1:
        target = int(sys.argv[1])
        server_op(target)
    else:
        cluster_op()
        
