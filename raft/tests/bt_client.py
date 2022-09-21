import sys
import socket
from raft.messages.status import StatusQueryMessage
from raft.messages.serializer import Serializer


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
        try:
            result = data.decode('utf-8')
        except UnicodeDecodeError:
            result = Serializer.deserialize(data)
        return result

    def get_status(self):
        sqm = StatusQueryMessage(self._addr, self._server_addr, None, None)
        data = Serializer.serialize(sqm)
        self._sock.sendto(data, self._server_addr)
        return self.get_result()
        
    def do_query(self):
        self._sock.sendto("query".encode('utf-8'), self._server_addr)
        return self.get_result()

    def do_credit(self, amount):
        self._sock.sendto(f"credit {amount}".encode('utf-8'), self._server_addr)
        return self.get_result()

    def do_debit(self, amount):
        self._sock.sendto(f"debit {amount}".encode('utf-8'), self._server_addr)
        return self.get_result()
        
                          
