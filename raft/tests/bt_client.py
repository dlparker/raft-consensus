import sys
import socket
import time
import asyncio
import random
from raft.messages.status import StatusQueryMessage
from raft.messages.command import ClientCommandMessage
from raft.messages.serializer import Serializer
from raft.comms.memory_comms import queues, Wrapper

def get_internal_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

class UDPBankTellerClient:
    
    def __init__(self, server_host, server_port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.settimeout(1)
        self._sock.bind(("", 0))
        port = self._sock.getsockname()[1]
        addr = get_internal_ip()
        self._addr = (addr, port)
        self._server_addr = server_host, server_port

    def get_result(self):
        try:
            data = self._sock.recv(1024)
        except OSError:
            raise RuntimeError("message reply timeout")
        result = Serializer.deserialize(data)
        if result.is_type("command_result"):
            return result.data
        return result

    def get_status(self):
        sqm = StatusQueryMessage(self._addr, self._server_addr, None, None)
        data = Serializer.serialize(sqm)
        self._sock.sendto(data, self._server_addr)
        return self.get_result()
        
    def do_query(self):
        qm = ClientCommandMessage(self._addr, self._server_addr,
                                  None, "query")
        data = Serializer.serialize(qm)
        self._sock.sendto(data, self._server_addr)
        return self.get_result()

    def do_credit(self, amount):
        cm = ClientCommandMessage(self._addr, self._server_addr,
                                  None, f"credit {amount}")
        data = Serializer.serialize(cm)
        self._sock.sendto(data, self._server_addr)
        return self.get_result()

    def do_debit(self, amount):
        dm = ClientCommandMessage(self._addr, self._server_addr,
                                  None, f"dedit {amount}")
        data = Serializer.serialize(dm)
        self._sock.sendto(data, self._server_addr)
        return self.get_result()
        
                          
class MemoryBankTellerClient:
    
    def __init__(self, server_host, server_port):


        self._server_addr = server_host, server_port
        self.queue = asyncio.Queue()
        global queues
        self.queues = queues
        poss = int(random.uniform(0, server_port - 100))
        while poss in self.queues:
            poss = int(random.uniform(0, server_port - 100))
        self._addr = ('localhost', poss)
        self.queues[self._addr] = self.queue
        self.queues[self._addr] = self.queue
        self.channel = None

    def get_channel(self):
        if self.channel is None:
            self.channel = queues[self._server_addr]
        return self.channel

    def do_credit(self, amount):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self._do_credit(amount))
        
    def do_debit(self, amount):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self._do_debug(amount))

    def do_query(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self._do_query())

    def get_status(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self._get_status())

    def get_result(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self._get_result())
        
    async def _get_result(self):
        start_time = time.time()
        w = None
        while time.time() - start_time < 2:
            if not self.queue.empty():
                w = await self.queue.get()
                break
            asyncio.sleep(0.01)
        if not w:
            raise Exception("timeout")
        data = w.data
        result = Serializer.deserialize(data)
        if result.is_type("command_result"):
            return result.data
        return result

    async def _get_status(self):
        sqm = StatusQueryMessage(self._addr, self._server_addr, None, None)
        data = Serializer.serialize(sqm)
        w = Wrapper(data, self._addr)
        await self.get_channel().put(w)
        return await self._get_result()
        
    async def _do_query(self):
        qm = ClientCommandMessage(self._addr, self._server_addr,
                                  None, "query")
        data = Serializer.serialize(qm)
        w = Wrapper(data, self._addr)
        await self.get_channel().put(w)
        return await self._get_result()

    async def _do_credit(self, amount):
        cm = ClientCommandMessage(self._addr, self._server_addr,
                                  None, f"credit {amount}")
        data = Serializer.serialize(cm)
        w = Wrapper(data, self._addr)
        await self.get_channel().put(w)
        return await self._get_result()

    async def _do_debit(self, amount):
        dm = ClientCommandMessage(self._addr, self._server_addr,
                                  None, f"dedit {amount}")
        data = Serializer.serialize(dm)
        w = Wrapper(data, self._addr)
        await self.get_channel().put(w)
        return await self._get_result()
        
                          
