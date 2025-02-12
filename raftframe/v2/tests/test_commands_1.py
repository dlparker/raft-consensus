#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

logging.basicConfig(level=logging.DEBUG)

from raftframe.v2.tests.servers import PausingCluster
from raftframe.v2.tests.servers import WhenMessageOut, WhenMessageIn
from raftframe.v2.tests.servers import WhenInMessageCount
from raftframe.v2.tests.servers import WhenAllMessagesForwarded, WhenAllInMessagesHandled

async def test_command_1():
    cluster = PausingCluster(3)
    cluster.set_configs()
    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]

    logger = logging.getLogger(__name__)
    await cluster.start()
    await ts_3.hull.start_campaign()
    ts_1.set_condition(WhenElectionDone())
    ts_2.set_condition(WhenElectionDone())
    ts_3.set_condition(WhenElectionDone())
        
    await asyncio.gather(ts_1.run_till_conditions(),
                         ts_2.run_till_conditions(),
                         ts_3.run_till_conditions())
    
    ts_1.clear_conditions()
    ts_2.clear_conditions()
    ts_3.clear_conditions()
    assert ts_3.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_3
    assert ts_2.hull.state.leader_uri == uri_3
    logger = logging.getLogger(__name__)
    logger.error('----------------------------------------------------------')
    await cluster.start_auto_comms()

    command_result = await ts_3.hull.apply_command("add 1")
    res1,err1 = command_result['result']
    assert res1 is not None
    assert err1 is None
    assert ts_1.operations.total == 1
    assert ts_2.operations.total == 1
    assert ts_3.operations.total == 1

    command_result = await ts_1.hull.apply_command("add 1")
    assert command_result['redirect'] == uri_3
    

    
    # FIXME!:
    # need to try to send a command to a candidate
    
