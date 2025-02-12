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
from raftframe.v2.tests.servers import WhenIsLeader, WhenHasLeader
from raftframe.v2.tests.servers import WhenElectionDone
from raftframe.v2.tests.servers import WhenAllMessagesForwarded, WhenAllInMessagesHandled
from raftframe.v2.tests.servers import WhenInMessageCount


async def test_stepwise_election_1():
    """This test is mainly for the purpose of testing the test support
        features implemented in the PausingCluster, PausingServer and
        various Condition implementations. Other tests already proved
        the basic election process using more granular control
        methods, this does the same kind of thing but using the
        run_till_condition model of controlling the code. It is still somewhat
        granular, and serves as a demo of how to build tests that can run 
        things, stop em, examine state, and continue
    """
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
    out1 = WhenMessageOut(RequestVoteMessage.get_code(),
                          message_target=uri_1, flush_when_done=False)
    ts_3.add_condition(out1)
    out2 = WhenMessageOut(RequestVoteMessage.get_code(),
                          message_target=uri_2, flush_when_done=False)
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

async def test_run_to_election_1():
    """This test is mainly for the purpose of testing the test support
        features implemented in the PausingCluster, PausingServer and
        various Condition implementations. This test shows how to use
        the least granular style of control, just allowing everything
        (except timers) proceed normally until the election is complete.
    """
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

    logger.debug("-------- Initial election completion pause test completed starting reelection")
    # now have leader resign, by telling it to become follower
    await ts_3.hull.demote_and_handle(None)
    assert ts_3.hull.get_state_code() == "FOLLOWER"
    # simulate timeout on heartbeat on only one follower, so it should win
    await ts_2.hull.state.leader_lost()
    
    ts_1.set_condition(WhenElectionDone())
    ts_2.set_condition(WhenElectionDone())
    ts_3.set_condition(WhenElectionDone())
        
    await asyncio.gather(ts_1.run_till_conditions(),
                         ts_2.run_till_conditions(),
                         ts_3.run_till_conditions())
    
    ts_1.clear_conditions()
    ts_2.clear_conditions()
    ts_3.clear_conditions()

    assert ts_2.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_2
    assert ts_3.hull.state.leader_uri == uri_2
    logger.debug("-------- Re-election test done")

    
