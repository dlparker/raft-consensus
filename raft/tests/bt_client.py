import sys
from socket import *

class UDPBankTellerClient:
    
    def __init__(self, server_host, server_port):
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.settimeout(1)

        self._server_addr = server_host, server_port

    def get_result(self):
        try:
            data = self._sock.recv(1024)
        except OSError:
            raise RuntimeError("message reply timeout")
        return data.decode('utf-8')
    
    def do_query(self):
        self._sock.sendto("query".encode('utf-8'), self._server_addr)
        return self.get_result()

    def do_credit(self, amount):
        self._sock.sendto(f"credit {amount}".encode('utf-8'), self._server_addr)
        return self.get_result()

    def do_debit(self, amount):
        self._sock.sendto(f"debit {amount}".encode('utf-8'), self._server_addr)
        return self.get_result()
        
                          
