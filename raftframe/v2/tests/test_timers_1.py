#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage


from raftframe.v2.tests.servers import WhenElectionDone
from raftframe.v2.tests.servers import PausingCluster, cluster_maker
from raftframe.v2.tests.servers import setup_logging

setup_logging()

async def test_heartbeat_1(cluster_maker):
    cluster = cluster_maker(3)
    heartbeat_period = 0.01
    config = cluster.build_cluster_config(heartbeat_period=heartbeat_period)
    cluster.set_configs(config)
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

    # Test that heartbeat happens in approx expected time
    start_time = time.time()
    full_in_ledger = []
    full_out_ledger = []
    fraction = heartbeat_period/10.0
    while time.time() - start_time < heartbeat_period  * 2:
        in_ledger, out_ledger = await cluster.deliver_all_pending()
        full_in_ledger += in_ledger
        full_out_ledger += out_ledger
        if len(full_in_ledger) > 1 and len(full_out_ledger) > 1:
            break
        await asyncio.sleep(fraction)
    assert full_out_ledger[0].get_code() == AppendEntriesMessage.get_code()
    assert full_in_ledger[0].get_code() == AppendEntriesMessage.get_code()
    assert full_out_ledger[1].get_code() == AppendResponseMessage.get_code()
    assert full_in_ledger[1].get_code() == AppendResponseMessage.get_code()
    
async def test_lost_leader_1(cluster_maker):
    cluster = cluster_maker(3)
    # make leader too slow, will cause re-election
    heartbeat_period = 0.05
    leader_lost_timeout = 0.01
    config = cluster.build_cluster_config(heartbeat_period=heartbeat_period,
                                          leader_lost_timeout=leader_lost_timeout)
    cluster.set_configs(config)
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

    # Test that election happens in appoximately the leader_lost timeout
    start_time = time.time()
    fraction = leader_lost_timeout/10.0
    while time.time() - start_time < leader_lost_timeout  + (fraction * 2):
        await cluster.deliver_all_pending()
        if (ts_1.hull.state.state_code == "LEADER"
            or ts_2.hull.state.state_code == "LEADER"
            or ts_3.hull.state.state_code == "LEADER"):
            break
        await asyncio.sleep(fraction)
    
    assert (ts_1.hull.state.state_code == "LEADER"
            or ts_2.hull.state.state_code == "LEADER"
            or ts_3.hull.state.state_code == "LEADER")
    
