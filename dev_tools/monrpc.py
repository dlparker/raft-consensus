import threading
import asyncio
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class MonRpcThread(threading.Thread):

    def __init__(self, port=8000):
        threading.Thread.__init__(self)
        self.port = port

    def run(self):
        # Create server
        with SimpleXMLRPCServer(('localhost', self.port),
                                requestHandler=RequestHandler) as server:
            server.register_introspection_functions()

            @server.register_function(name="ping")
            def ping():
                print("got ping", flush=True)
                
                return "pong"
            # Run the server's main loop
            server.serve_forever()

class MainThread:

    def __init__(self):
        self.thread = MonRpcThread()

    async def looper(self):
        counter = 0
        while True:
            await asyncio.sleep(1)
        
    def run(self):
        self.thread.start()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self.looper())
        
    

if __name__ == '__main__':
    MainThread().run()
