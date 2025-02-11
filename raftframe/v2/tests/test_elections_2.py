#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

logging.basicConfig(level=logging.DEBUG)

from raftframe.v2.tests.servers import cluster_of_three,cluster_of_five
from raftframe.v2.tests.servers import WhenMessageOut, WhenMessageIn
from raftframe.v2.tests.servers import WhenIsLeader, WhenHasLeader
from raftframe.v2.tests.servers import WhenAllMessagesForwarded, WhenAllInMessagesHandled

async def test_stepwise_election_1(cluster_of_three):
    cluster = cluster_of_three
    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]

    logger = logging.getLogger(__name__)
    await cluster.start()
    await ts_3.hull.start_campaign()
    out1 = WhenMessageOut(RequestVoteMessage.get_code(),
                        message_target=uri_1)
    ts_3.add_condition(out1)
    out2 = WhenMessageOut(RequestVoteMessage.get_code(),
                        message_target=uri_2)
    ts_3.add_condition(out2)
    await ts_3.run_till_conditions()
    ts_3.clear_conditions()
    # Candidate is poised to send request for vote to other two servers
    # let the messages go out
    candidate = ts_3.hull.state
    logger.debug("Candidate posted vote requests for term %d", candidate.log.get_term())
    logger.debug("ts_1 term %d", ts_1.log.get_term())
    logger.debug("ts_2 term %d", ts_1.log.get_term())

    # let just these messages go
    ts_3.set_condition(WhenAllMessagesForwarded())
    await ts_3.run_till_conditions()
    ts_3.clear_conditions()

    # Wait for the other services to send their vote responses back
    ts_1.set_condition(WhenMessageIn(RequestVoteMessage.get_code()))
    ts_2.set_condition(WhenMessageIn(RequestVoteMessage.get_code()))
    await ts_1.run_till_conditions()
    ts_1.clear_conditions()
    await ts_2.run_till_conditions()
    ts_2.clear_conditions()

    logger.debug("Followers have messages pending")
    ts_1.set_condition(WhenAllInMessagesHandled())
    ts_2.set_condition(WhenAllInMessagesHandled())
    await ts_1.run_till_conditions()
    await ts_2.run_till_conditions()

    logger.debug("Followers outgoing vote response messages pending")

    assert ts_1.hull.state.last_vote == uri_3
    assert ts_2.hull.state.last_vote == uri_3
    # Let all the messages fly until delivered
    await cluster.deliver_all_pending()
    assert ts_3.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_3
    assert ts_2.hull.state.leader_uri == uri_3

    logger.debug("Stepwise paused election test completed")
    
async def test_stepwise_election_1(cluster_of_three):
    cluster = cluster_of_three
    uri_1 = cluster.node_uris[0]
    uri_2 = cluster.node_uris[1]
    uri_3 = cluster.node_uris[2]

    ts_1 = cluster.nodes[uri_1]
    ts_2 = cluster.nodes[uri_2]
    ts_3 = cluster.nodes[uri_3]

    logger = logging.getLogger(__name__)
    await cluster.start()
    await ts_3.hull.start_campaign()

    ts_1.set_condition(WhenMessageOut(AppendResponseMessage.get_code()))
    ts_2.set_condition(WhenMessageOut(AppendResponseMessage.get_code()))

    ts_3.add_condition(WhenMessageIn(AppendResponseMessage.get_code(), uri_1))
    ts_3.add_condition(WhenMessageIn(AppendResponseMessage.get_code(), uri_2))
    await asyncio.gather(ts_1.run_till_conditions(),
                         ts_2.run_till_conditions(),
                         ts_3.run_till_conditions())

    ts_3.clear_conditions()
    ts_3.set_condition(WhenAllMessagesForwarded())
    await ts_3.run_till_conditions()
    assert ts_3.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_3
    assert ts_2.hull.state.leader_uri == uri_3

    logger.debug("Election completion pause test completed")
    
