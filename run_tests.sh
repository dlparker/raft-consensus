#!/bin/bash
set -x
export PYTHONBREAKPOINT=ipdb.set_trace
#export PYTHONPATH="`pwd`:$(dirname `pwd`)"
export PYTHONPATH="."

if [ -z ${VIRTUAL_ENV+x} ]; then
   source .venv/bin/activate
fi    
if [[ `which pytest` != $VIRTUAL_ENV/bin/pytest ]]; then
   source .venv/bin/activate
fi
if [ $# -eq 0 ]; then
    pytest --verbose  --cov-config=coverage.cfg  --cov-report=html --cov-report=term -x --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb -s --cov=raftframe tests
else
    pytest --verbose  --cov-config=coverage.cfg --cov-report=html --cov-report=term -x --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb -s  --cov=raftframe $@
fi
