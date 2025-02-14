#!/usr/bin/env python
import os
import sys
import time
import traceback
from pathlib import Path
import shutil
import subprocess

root_dir = Path(__file__).parent.parent.parent
    
konsole = Path('/usr/bin/konsole')
script_dir = Path(root_dir, 'dev_tools')
if konsole.exists():
    #terminal_command_prefix = ['konsole', '--workdir', str(script_dir), '--hold', '-e']
    terminal_command_prefix = ['konsole', '--workdir', str(script_dir), '-e']
else:
    # gnome_terminal = Path('/usr/bin/gnome_terminal')
    raise Exception('need code for this distribution')

ops_command = [ sys.executable,  'pcluster_examples/cluster_and_client.py']
os.environ["RPC_MONITOR"] = '1'

c = terminal_command_prefix + ops_command
print(c)
p1 = subprocess.Popen(c)

mon_command = ops_command = [ sys.executable,  'monrpc.py']
m = terminal_command_prefix + mon_command
time.sleep(1)
print(m)
p2 = subprocess.Popen(m)

        
