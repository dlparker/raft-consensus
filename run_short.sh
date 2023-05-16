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
      raftframe/tests/test_basic.py \
      raftframe/tests/test_candidate_edges.py \
      raftframe/tests/test_delayed_start.py \
      raftframe/tests/test_mem_comms.py \
      raftframe/tests/test_ps_cluster.py \
      raftframe/tests/test_server_edges.py \
      raftframe/tests/test_state_map.py \
      raftframe/tests/test_udp_comms.py \
      raftframe/tests/test_log_pulls.py \
      raftframe/tests/test_follower_edges.py \
      raftframe/tests/test_leader_edges.py \
      raftframe/tests/test_backdown.py


if [ -z ${DO_COVERAGE+x} ]; then
    foo=""
else
    coverage combine --rcfile=`pwd`/raftframe/coverage.cfg --append
    coverage html --rcfile=`pwd`/raftframe/coverage.cfg
    coverage report --rcfile=`pwd`/raftframe/coverage.cfg
fi    
