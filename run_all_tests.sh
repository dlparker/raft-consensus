#!/bin/bash
set -x
export PYTHONBREAKPOINT=ipdb.set_trace
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PYTHONPATH=`pwd`

if [ -z ${VIRTUAL_ENV+x} ]; then
   source .venv/bin/activate
fi    
if [[ `which pytest` != $VIRTUAL_ENV/bin/pytest ]]; then
   source .venv/bin/activate
fi
rm -rf /tmp/test_logs
mkdir -p /tmp/test_log
export OVERRIDE_LOG_CONFIG_FOR_TESTS=/tmp/test_logs
 
pytest --verbose --cov=raft --cov-config=`pwd`/raft/coverage.cfg --cov-report=html \
       --cov-report=term  -x --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb \
#       --log-cli-level=DEBUG \
#      --log-cli-format='%(process)d %(asctime)s [%(levelname)s] %(name)s: %(message)s'\
       raft/tests 
coverage combine --rcfile=`pwd`/raft/coverage.cfg --append
coverage html --rcfile=`pwd`/raft/coverage.cfg
coverage report --rcfile=`pwd`/raft/coverage.cfg
unset OVERRIDE_LOG_CONFIG_FOR_TESTS
