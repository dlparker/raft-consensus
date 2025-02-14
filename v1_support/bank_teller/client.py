#!/usr/bin/env python
import sys
from pathlib import Path
sdir = Path(__file__).parent.resolve()
basedir = sdir.parent
raft_dir = Path(basedir, 'raftframe')
dev_dir = Path(basedir, "dev_tools")
sys.path.append(basedir.as_posix())
sys.path.append(dev_dir.as_posix())
print(sys.path)

from dev_tools.bt_client import UDPBankTellerClient


def run_client(server_host, server_port):
    client = UDPBankTellerClient(server_host, server_port)

    cmd = ''

    print('Welcome to your ATM! You can check your balance, credit to or debit from your account')
    print('Your starting balance is 0')
    print('Available commands are: query, credit <amount>, debit <amount>')

    while cmd != 'exit':
        cmd = input('Enter command: ')
        toks = cmd.split()
        if toks[0] =='query':
            result = client.do_query()
        elif toks[0] =='credit':
            result = client.do_credit(toks[1])
        elif toks[0] =='debit':
            result = client.do_debit(toks[1])
        elif toks[0] =='exit':
            break
        else:
            print(f"confused by command {cmd}")
            continue
        print(result['response'])


if __name__=="__main__":
    import argparse
    def create_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument('-p', '--port', required=False, default=5000,
                            help="port on which server listens")
        parser.add_argument('-i', '--ip_addr', required=False, default="localhost",
                            help="IPV4 address of server")
        return parser
        
    parser = create_parser()
    args = parser.parse_args()
    try:
        run_client(args.ip_addr, int(args.port))
    except KeyboardInterrupt:
        print('\nClient terminated')
        pass
