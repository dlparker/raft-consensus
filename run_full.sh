#!/bin/bash
set -x
export PYTHONBREAKPOINT=ipdb.set_trace
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PYTHONPATH=`pwd`

if [ -z ${VIRTUAL_ENV+x} ]; then
   source .venv/bin/activate
fi
if [ -z ${TEST_LOGGING+x} ]; then
    $LOG_OPTION=""
else
    $LOG_OPTION=$(-p no:logging)
fi    
if [ -z ${PYTEST_NO_STOP+x} ]; then
    STOP_OPTION="-x --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb"
else
    STOP_OPTION=""
fi    
if [ -z ${DO_COVERAGE+x} ]; then
    COVER_OPTION=""
else
    COVER_OPTION="--cov=raft --cov-config=`pwd`/raft/coverage.cfg --cov-report=html --cov-report=term"
fi    

if [[ `which pytest` != $VIRTUAL_ENV/bin/pytest ]]; then
   source .venv/bin/activate
fi
rm -rf /tmp/raft_tests
mkdir -p /tmp/raft_tests
 
pytest --verbose \
       $COVER_OPTION \
       $STOP_OPTION \
       $LOG_OPTION \
      -s \
      raft/tests/test_basic.py \
      raft/tests/test_mem_comms.py \
      raft/tests/test_udp_comms.py \
      raft/tests/test_log_ops.py \
      raft/tests/test_state_map.py \
      raft/tests/test_server_edges.py \
      raft/tests/test_candidate_edges.py \
      raft/tests/test_follower_edges.py \
      raft/tests/test_leader_edges.py \
      raft/tests/test_ups_n_downs.py


if [ -z ${DO_COVERAGE+x} ]; then
    foo=""
else
    coverage combine --rcfile=`pwd`/raft/coverage.cfg --append
    coverage html --rcfile=`pwd`/raft/coverage.cfg
    coverage report --rcfile=`pwd`/raft/coverage.cfg
fi    
