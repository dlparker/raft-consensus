#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

logging.basicConfig(level=logging.DEBUG)

from raftframe.v2.tests.servers import WhenMessageOut, WhenMessageIn
from raftframe.v2.tests.servers import WhenInMessageCount, WhenElectionDone
from raftframe.v2.tests.servers import WhenAllMessagesForwarded, WhenAllInMessagesHandled
from raftframe.v2.tests.servers import PausingCluster, cluster_maker

async def test_command_1(cluster_maker):
    cluster = cluster_maker(3)
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
    ts_1.set_trigger(WhenElectionDone())
    ts_2.set_trigger(WhenElectionDone())
    ts_3.set_trigger(WhenElectionDone())
        
    await asyncio.gather(ts_1.run_till_triggers(),
                         ts_2.run_till_triggers(),
                         ts_3.run_till_triggers())
    
    ts_1.clear_triggers()
    ts_2.clear_triggers()
    ts_3.clear_triggers()
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
    term = ts_3.hull.log.get_term()
    index = ts_3.hull.log.get_last_index()
    assert index == 1
    assert ts_1.hull.log.get_term() == term
    assert ts_1.hull.log.get_last_index() == index
    assert ts_2.hull.log.get_term() == term
    assert ts_2.hull.log.get_last_index() == index
    
    command_result = await ts_1.hull.apply_command("add 1")
    assert command_result['redirect'] == uri_3
    
    await ts_1.hull.state.leader_lost()
    assert ts_1.hull.get_state_code() == "CANDIDATE"
    command_result = await ts_1.hull.apply_command("add 1")
    assert command_result['retry'] is not None
    
    assert ts_1.hull.log.get_last_index() == index
    assert ts_2.hull.log.get_last_index() == index
    ts_1.set_trigger(WhenElectionDone())
    ts_2.set_trigger(WhenElectionDone())
    ts_3.set_trigger(WhenElectionDone())
        
    await asyncio.gather(ts_1.run_till_triggers(),
                         ts_2.run_till_triggers(),
                         ts_3.run_till_triggers())
    
    ts_1.clear_triggers()
    ts_2.clear_triggers()
    ts_3.clear_triggers()
    
    assert ts_1.hull.get_state_code() == "LEADER"
    assert ts_2.hull.state.leader_uri == uri_1
    assert ts_3.hull.state.leader_uri == uri_1

