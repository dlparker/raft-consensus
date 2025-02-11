#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

logging.basicConfig(level=logging.DEBUG)

from raftframe.v2.tests.scaffolds import cluster_of_three,cluster_of_five

async def test_command_1(cluster_of_three):
    cluster = cluster_of_three
    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]

    ts_1.hull.config.cluster.leader_lost_timeout = 1000
    ts_2.hull.config.cluster.leader_lost_timeout = 1000
    ts_3.hull.config.cluster.leader_lost_timeout = 1000

    # make sure that we can control timeouts and get
    # things to happend that way

    await cluster.start()
    await ts_3.hull.start_campaign()
    assert len(ts_1.in_messages) == 1
    assert len(ts_2.in_messages) == 1
    assert ts_1.in_messages[0].get_code() == RequestVoteMessage.get_code()
    assert ts_2.in_messages[0].get_code() == RequestVoteMessage.get_code()
    
    # vote requests, then vote responses
    await cluster.deliver_all_pending()
    assert ts_3.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_3
    assert ts_2.hull.state.leader_uri == uri_3

    logger = logging.getLogger(__name__)
    logger.error('----------------------------------------------------------')
    await cluster.start_auto_comms()

    res1,err1 = await ts_3.hull.state.apply_command("add 1")
    assert res1 is not None
    assert err1 is None
    assert ts_1.command_processor.total == 1
    assert ts_2.command_processor.total == 1
    assert ts_3.command_processor.total == 1

    
    
    
