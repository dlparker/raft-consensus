import sys
import xmlrpc.client

port = sys.argv[1]
with xmlrpc.client.ServerProxy(f'http://localhost:{port}') as s:
    # Print list of available methods
    print(s.system.listMethods())
    print(s.ping())  

