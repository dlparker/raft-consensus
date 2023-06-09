"""

"""
from socket import *
import time
import asyncio
import threading
import queue
import logging
import traceback
from collections import defaultdict
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client

from raftframe.utils import task_logger
from raftframe.comms.comms_api import CommsAPI

class XMLRPCComms(CommsAPI):
    
    started = False
    def __init__(self):
        self.channels = {}
        self.endpoint = None
        self.thread = None
        self.server = None
        self.serializer = None
        self.out_queue = queue.Queue()
        self.in_queue = asyncio.Queue()
        self.thread = None
        self.reader_task = None
        self.keep_running = False
        self.logger = logging.getLogger(__name__)

    async def start(self, server, endpoint):
        if self.started:   # pragma: no cover error
            raise Exception("can call start only once")
        self.logger = logging.getLogger(__name__)
        self.server = server
        self.endpoint = endpoint
        self.serializer = self.server.get_comms_serializer()
        self.keep_running = True
        self.server_thread = ServerThread(self, endpoint, server, self.in_queue, self.logger)
        self.server_thread.start()
        self.client_thread = ClientThread(self, self.out_queue, self.serializer, self.logger)
        self.client_thread.start()
        self.reader_task = task_logger.create_task(self.in_reader(),
                                                   logger=self.logger,
                                                   message="comms listener error")
        await asyncio.sleep(0)
        self.started = True
        self.logger.info('XMLRPC Listening on %s', self.endpoint)
        self.logger.debug('Comms created in thread %s', threading.get_native_id())

    async def stop(self):
        if self.server_thread:
            self.server_thread.stop()
            self.server_thread = None
        if self.client_thread:
            self.client_thread.stop()
            self.client_thread = None
        if self.reader_task:
            self.reader_task.cancel()
            await asyncio.sleep(0)
        self.logger.debug("stop complete in thread %s", threading.get_native_id())
            
    async def post_message(self, message):
        # make sure addresses are tuples
        message._sender = (message.sender[0], message.sender[1])
        message._receiver = (message.receiver[0], message.receiver[1])
        if not isinstance(message, dict):
            self.logger.debug("posting %s to %s",
                              message, message.receiver)
        self.out_queue.put_nowait(message)

    def are_out_queues_empty(self):
        return self.out_queue.empty()
        
    async def in_reader(self):
        self.logger.debug('reader task running')
        while self.keep_running:
            try:
                try:
                    data = self.in_queue.get_nowait()
                except asyncio.QueueEmpty:
                    try:
                        await asyncio.sleep(0.01)
                    except Exception:
                        self.logger.debug(traceback.format_exc())
                    continue
                #self.logger.debug('reader task got data from in queue %s', data)
            except asyncio.exceptions.CancelledError: # pragma: no cover error
                break
            except Exception as e:  # pragma: no cover error
                self.logger.error(traceback.format_exc())
                self.logger.error("cannot get queue data")
                continue
            try:
                message = self.serializer.deserialize_message(data)
                #self.logger.debug('reader task got message %s', message)
            except Exception as e:  # pragma: no cover error
                self.logger.error(traceback.format_exc())
                self.logger.error("cannot deserialze incoming data '%s...'",
                                  data[:30])
                continue
            message._receiver = message.receiver[0], message.receiver[1]
            message._sender = message.sender[0], message.sender[1]
            await self.server.on_message(message)
        self.reader_task = None
        self.logger.debug('reader task done')
        

class ClientThread(threading.Thread):

    def __init__(self, comms, out_queue, serializer, logger):
        threading.Thread.__init__(self)
        self.comms = comms
        self.out_queue = out_queue
        self.serializer = serializer
        self.logger = logger
        self.keep_running = False
        self.clients = {}

    def add_client(self, endpoint):
        host, port = endpoint
        url = f'http://{host}:{port}'
        self.logger.debug("new client url %s", url)
        client = xmlrpc.client.ServerProxy(url, use_builtin_types=True)
        self.clients[endpoint] = client
        
    def stop(self):
        self.keep_running = False
        self.out_queue.put('diediedie')
        self.logger.debug("stopped client thread")
        
    def run(self):
        self.logger.debug('Comms client thread %s', threading.get_native_id())
        self.logger.debug('client thread starting')
        self.keep_running = True
        while self.keep_running:
            msg = self.out_queue.get()
            if msg == "diediedie":
                break
            self.logger.debug('client thread dequeued out message %s', msg)
            endpoint = msg.receiver[0], msg.receiver[1]
            if endpoint not in self.clients:
                try:
                    self.logger.error("adding client connection to endpoint %s", endpoint)
                    self.add_client(endpoint)
                except Exception as e:
                    self.logger.error("can't make client connection to endpoint %s", endpoint)
                    self.logger.error(traceback.format_exc())
                    continue
            try:
                client = self.clients[endpoint]
                #self.logger.debug('client thread serializing %s', msg)
                data = self.serializer.serialize_message(msg)
                #self.logger.debug('client thread serializing %s', data)
                client.new_msg(data)
            except Exception as e:
                self.logger.error("broken client connection to endpoint %s", endpoint)
                self.logger.error(traceback.format_exc())
                del self.clients[endpoint]
                continue
                
        self.logger.debug("client thread exiting thread %s", threading.get_native_id())
        
class ServerThread(threading.Thread):

    def __init__(self, comms, endpoint, raftframe_server, in_queue, logger):
        threading.Thread.__init__(self)
        self.comms = comms
        self.endpoint = endpoint
        self.raftframe_server = raftframe_server
        self.in_queue = in_queue
        self.logger = logger
        self.host,self.port = endpoint
        self.server = None

    def stop(self):
        if self.server:
            self.logger.debug("calling shutdown on xmlrpc server")
            self.server.shutdown()
        
    def run(self):
        self.logger.debug('Comms server thread %s', threading.get_native_id())
        self.logger.debug("starting server thread")
        
        # Create server
        try:
            with SimpleXMLRPCServer((self.host, self.port), logRequests=False,
                                    use_builtin_types=True) as server:
                self.server = server
                self.logger.info('XMLRPC server on %s %s', self.host, self.port)
                server.register_introspection_functions()
                
                @server.register_function(name="new_msg")
                def new_msg(data):
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    async def do_put():
                        #self.logger.debug("server thread posting to in_queue %s", data)
                        loop.call_soon_threadsafe(self.in_queue.put_nowait, data)
                        await asyncio.sleep(0)
                    loop.run_until_complete(do_put())
                    return "ok"
                # Run the server's main loop
                server.serve_forever()
        except Exception:
            self.logger.error("broken server thread")
            self.logger.error(traceback.format_exc())
        finally:
            self.logger.debug("server thread exiting thread %s", threading.get_native_id())
            self.server = None
            

