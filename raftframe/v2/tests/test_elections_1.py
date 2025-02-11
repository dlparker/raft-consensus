#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

logging.basicConfig(level=logging.DEBUG)

from raftframe.v2.tests.scaffolds import cluster_of_three,cluster_of_five

async def test_election_1(cluster_of_three):
    """ This is the happy path, everybody has same state, only one server runs for leader,
        everybody response correctly """

    cluster = cluster_of_three
    await cluster.start()
    uri_1 = cluster.node_uris[0]
    ts_1 = cluster.nodes[uri_1]
    uri_2 = cluster.node_uris[1]
    ts_2 = cluster.nodes[uri_2]
    uri_3 = cluster.node_uris[2]
    ts_3 = cluster.nodes[uri_3]

    # tell first one to start election, should send request vote messages to other two
    await ts_1.hull.start_campaign()
    assert len(ts_2.in_messages) == 1
    assert len(ts_3.in_messages) == 1
    assert ts_2.in_messages[0].get_code() == RequestVoteMessage.get_code()
    assert ts_3.in_messages[0].get_code() == RequestVoteMessage.get_code()

    # now deliver those, we should get two replies at first one, both with yes
    await ts_2.do_next_msg()
    await ts_3.do_next_msg()
    assert len(ts_1.in_messages) == 2
    assert ts_1.in_messages[0].get_code() == RequestVoteResponseMessage.get_code()
    assert ts_1.in_messages[1].get_code() == RequestVoteResponseMessage.get_code()
    
    # now let candidate process votes, should then promote itself
    await ts_1.do_next_msg()
    await ts_1.do_next_msg()
    assert ts_1.hull.get_state_code() == "LEADER"

    # leader should send append_entries to everyone else in cluster,
    # check for delivery pending
    assert len(ts_2.in_messages) == 1
    assert len(ts_3.in_messages) == 1
    assert ts_2.in_messages[0].get_code() == AppendEntriesMessage.get_code()
    assert ts_3.in_messages[0].get_code() == AppendEntriesMessage.get_code()
    # now deliver those, we should get two replies at first one,
    await ts_2.do_next_msg()
    await ts_3.do_next_msg()
    assert len(ts_1.in_messages) == 2
    assert ts_1.in_messages[0].get_code() == AppendResponseMessage.get_code()
    assert ts_1.in_messages[1].get_code() == AppendResponseMessage.get_code()


async def test_election_2(cluster_of_five):
    cluster = cluster_of_five
    await cluster.start()

    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]
    uri_4 = cluster.node_uris[4]
    uri_5 = cluster.node_uris[4]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]
    ts_4 = cluster.nodes[uri_4]
    ts_5 = cluster.nodes[uri_5]
    await ts_1.hull.start_campaign()
    # vote requests, then vote responses
    await cluster.deliver_all_pending()
    assert ts_1.hull.get_state_code() == "LEADER"
    # append entries, then responses
    await cluster.deliver_all_pending()
    assert ts_2.hull.state.leader_uri == uri_1
    assert ts_3.hull.state.leader_uri == uri_1
    assert ts_4.hull.state.leader_uri == uri_1
    assert ts_5.hull.state.leader_uri == uri_1

    
async def test_reelection_1(cluster_of_three):
    cluster = cluster_of_three
    await cluster.start()
    
    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]

    await ts_1.hull.start_campaign()
    # vote requests, then vote responses
    await cluster.deliver_all_pending()
    assert ts_1.hull.get_state_code() == "LEADER"
    # append entries, then responses
    await cluster.deliver_all_pending()
    assert ts_2.hull.state.leader_uri == uri_1
    assert ts_3.hull.state.leader_uri == uri_1

    # now have leader resign, by telling it to become follower
    await ts_1.hull.demote_and_handle(None)
    assert ts_1.hull.get_state_code() == "FOLLOWER"
    # pretend timeout on heartbeat on only one, ensuring it will win
    await ts_2.hull.state.leader_lost()
    await cluster.deliver_all_pending()
    assert ts_2.hull.get_state_code() == "LEADER"
    assert ts_3.hull.get_state_code() == "FOLLOWER"
    
async def test_reelection_2(cluster_of_three):
    cluster = cluster_of_three
    await cluster.start()
    
    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]

    await ts_1.hull.start_campaign()
    # vote requests, then vote responses
    await cluster.deliver_all_pending()
    assert ts_1.hull.get_state_code() == "LEADER"
    # append entries, then responses
    await cluster.deliver_all_pending()
    assert ts_2.hull.state.leader_uri == uri_1
    assert ts_3.hull.state.leader_uri == uri_1

    # now have leader resign, by telling it to become follower
    await ts_1.hull.demote_and_handle(None)
    assert ts_1.hull.get_state_code() == "FOLLOWER"
    # pretend timeout on heartbeat on only one followers, so it should win
    await ts_2.hull.state.leader_lost()
    await cluster.deliver_all_pending()
    assert ts_2.hull.get_state_code() == "LEADER"
    assert ts_1.hull.get_state_code() == "FOLLOWER"
    assert ts_3.hull.get_state_code() == "FOLLOWER"
    
async def test_reelection_3(cluster_of_three):
    cluster = cluster_of_three
    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]

    # make sure that we can control timeouts and get
    # things to happend that way

    # ensure that ts_3 wins first election
    ts_1.hull.config.cluster.leader_lost_timeout = 1
    ts_2.hull.config.cluster.leader_lost_timeout = 1
    ts_3.hull.config.cluster.leader_lost_timeout = 0.001

    # ensure that ts_2 wins re-election
    ts_1.hull.config.cluster.election_timeout_min = 1
    ts_1.hull.config.cluster.election_timeout_max = 1.2
    ts_2.hull.config.cluster.election_timeout_min = 0.001
    ts_2.hull.config.cluster.election_timeout_max = 0.0011
    ts_3.hull.config.cluster.election_timeout_min = 1
    ts_3.hull.config.cluster.election_timeout_max = 1.2
    
    await cluster.start()
    # give ts_3 time to timeout and start campaign
    await asyncio.sleep(0.001)
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
    logger.warning('setting up re-election')
    # tell leader to resign and manually trigger elections on all the
    # servers, ts_2 should win because of timeout
    await ts_3.hull.demote_and_handle(None)
    await ts_3.hull.start_campaign()
    logger.warning('leader ts_3 demoted and campaign started')
    # ts_2 started last, but should win and raise term to 3 because of timeout
    await ts_1.hull.start_campaign()
    logger.warning('ts_1 starting campaign')
    await ts_2.hull.start_campaign()
    logger.warning('ts_2 starting campaign, delivering messages')
    await cluster.deliver_all_pending()
    logger.warning('waiting for re-election to happend')
    start_time = time.time()
    while time.time() - start_time < 0.01:
        await cluster.deliver_all_pending()
        if ts_2.hull.get_state_code() == "LEADER":
            break
        await asyncio.sleep(0.0001)
        
    assert ts_2.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_2
    assert ts_3.hull.state.leader_uri == uri_2
    
    

    
    
    
