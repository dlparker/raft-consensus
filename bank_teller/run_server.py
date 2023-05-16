import os
import sys
from pathlib import Path
sdir = Path(__file__).parent.resolve()
basedir = sdir.parent
raft_dir = Path(basedir, 'raftframe')
test_dir = Path(raft_dir, "tests")
sys.path.append(basedir.as_posix())
sys.path.append(test_dir.as_posix())
print(sys.path)
from logging.config import dictConfig
wdir = os.getcwd()

from tests.bt_server import UDPBankTellerServer
from tests.log_control import config_logging

def run_server(ipv4, port, endpoints):
    config,_ = config_logging(f"server_{port}.log")
    dictConfig(config)
    print(endpoints)
    server = UDPBankTellerServer(port,
                                 os.getcwd(),
                                 f"server_{port}",
                                 endpoints, False)
    server.start()


if __name__ == '__main__':
    import argparse
    def create_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument('-p', '--port', required=False, default=5000,
                            help="port on which to listen")
        parser.add_argument('-i', '--ipv4', required=False, default="localhost",
                            help="IPV4 address on which to listen")
        parser.add_argument('-n', '--node_list', required=True,
                            help="Enpoints of other nodes, e.g. 127.0.0.1:5000,127.0.0.1:5001,192.168.100.1:5000")
        
        return parser
        
    parser = create_parser()
    args = parser.parse_args()
    nodes = args.node_list.split(",")
    endpoints = [ (h.split(":")[0],int(h.split(":")[1])) for h in nodes ]
    try:
        run_server(args.ipv4, int(args.port), endpoints)
    except KeyboardInterrupt:
        print('\nServer terminated')
        pass
