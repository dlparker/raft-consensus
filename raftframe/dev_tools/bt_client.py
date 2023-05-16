import sys
import socket
import time
import asyncio
from raftframe.messages.status import StatusQueryMessage
from raftframe.messages.command import ClientCommandMessage
from raftframe.serializers.msgpack import MsgpackSerializer as Serializer
from raftframe.dev_tools.memory_comms import get_channels, add_client, Wrapper

def get_internal_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

class UDPBankTellerClient:
    
    def __init__(self, server_host, server_port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2)
        self.sock.bind(("", 0))
        self.port = self.sock.getsockname()[1]
        self.host = get_internal_ip()
        self.addr = (self.host, self.port)
        self.server_addr = server_host, server_port

    def __str__(self):
        return f"client_for_{self.port}"
    
    def get_result(self):
        try:
            data = self.sock.recv(1024)
        except OSError:
            raise RuntimeError("message reply timeout")
        result = Serializer.deserialize_message(data)
        if result.is_type("command_result"):
            return result.data
        return result

    def get_status(self):
        sqm = StatusQueryMessage(self.addr, self.server_addr, None, None)
        data = Serializer.serialize_message(sqm)
        self.sock.sendto(data, self.server_addr)
        return self.get_result()
        
    def do_query(self):
        qm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, "query")
        data = Serializer.serialize_message(qm)
        self.sock.sendto(data, self.server_addr)
        return self.get_result()

    def do_log_stats(self):
        qm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, "log_stats")
        data = Serializer.serialize_message(qm)
        self.sock.sendto(data, self.server_addr)
        return self.get_result()

    def do_credit(self, amount):
        cm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, f"credit {amount}")
        data = Serializer.serialize_message(cm)
        self.sock.sendto(data, self.server_addr)
        return self.get_result()

    def do_debit(self, amount):
        dm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, f"debit {amount}")
        data = Serializer.serialize_message(dm)
        self.sock.sendto(data, self.server_addr)
        return self.get_result()
        
                          
class MemoryBankTellerClient:
    
    def __init__(self, server_host, server_port):
        self.server_addr = server_host, server_port
        self.queue = asyncio.Queue()
        self.client = add_client(self.queue)
        self.addr = self.client.addr
        self.channel = None
        self.timeout = 2

    def set_timeout(self, timeout):
        self.timeout = timeout

    async def get_channel(self):
        # need to not blow up if server is not running
        if self.channel is None:
            self.channel = get_channels().get(self.server_addr, None)
        if self.channel is None:
            start_time = time.time()
            while time.time() - start_time < 2:
                self.channel = get_channels().get(self.server_addr, None)
                if self.channel:
                    break
                await asyncio.sleep(0.01)
        if self.channel is None:
            raise Exception("timeout")
        return self.channel.queue

    def do_credit(self, amount):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.a_do_credit(amount))
        
    def do_debit(self, amount):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.a_do_debit(amount))

    def do_query(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.a_do_query())

    def do_log_stats(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.a_do_log_stats())

    def get_status(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.a_get_status())

    def direct_message(self, message):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.a_direct_message(message))
        
    def get_result(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.a_get_result())
        
    async def a_get_result(self):
        w = None
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            if not self.queue.empty():
                w = await self.queue.get()
                break
            await asyncio.sleep(0.001)
            xtime = time.time()
        if not w:
            raise Exception(f"timeout after {xtime - start_time}")
        data = w.data
        result = Serializer.deserialize_message(data)
        if result.is_type("command_result"):
            return result.data
        return result

    async def a_get_status(self):
        sqm = StatusQueryMessage(self.addr, self.server_addr, None, None)
        data = Serializer.serialize_message(sqm)
        w = Wrapper(data, self.addr)
        await (await self.get_channel()).put(w)
        return await self.a_get_result()
        
    async def a_do_query(self):
        qm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, "query")
        data = Serializer.serialize_message(qm)
        w = Wrapper(data, self.addr)
        await (await self.get_channel()).put(w)
        return await self.a_get_result()

    async def a_do_log_stats(self):
        qm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, "log_stats")
        data = Serializer.serialize_message(qm)
        w = Wrapper(data, self.addr)
        await (await self.get_channel()).put(w)
        return await self.a_get_result()

    async def a_do_credit(self, amount):
        cm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, f"credit {amount}")
        data = Serializer.serialize_message(cm)
        w = Wrapper(data, self.addr)
        await (await self.get_channel()).put(w)
        return await self.a_get_result()

    async def a_do_debit(self, amount):
        dm = ClientCommandMessage(self.addr, self.server_addr,
                                  None, f"debit {amount}")
        data = Serializer.serialize_message(dm)
        w = Wrapper(data, self.addr)
        await (await self.get_channel()).put(w)
        return await self.a_get_result()

    async def a_direct_message(self, message):
        data = Serializer.serialize_message(message)
        w = Wrapper(data, self.addr)
        await (await self.get_channel()).put(w)
        
                          
