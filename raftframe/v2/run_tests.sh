#!/bin/bash
#set -x
export PYTHONBREAKPOINT=ipdb.set_trace
part1="$(dirname `pwd`)"
export PYTHONPATH="$(dirname $part1)"

if [ -z ${VIRTUAL_ENV+x} ]; then
   source .venv/bin/activate
fi    
if [[ `which pytest` != $VIRTUAL_ENV/bin/pytest ]]; then
   source .venv/bin/activate
fi
if [ $# -eq 0 ]; then
    pytest --verbose  --cov-config=`pwd`/coverage.cfg  --cov-report=html --cov-report=term -x --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb -s --cov=raftframe.v2.states --cov=raftframe.v2.hull ./tests
else
    pytest --verbose  --cov-config=`pwd`/coverage.cfg --cov-report=html --cov-report=term -x --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb -s --cov=raftframe.v2.states --cov=raftframe.v2.hull $@
fi
