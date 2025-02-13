#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

logging.basicConfig(level=logging.DEBUG)

from raftframe.v2.tests.servers import WhenMessageOut, WhenMessageIn
from raftframe.v2.tests.servers import WhenIsLeader, WhenHasLeader
from raftframe.v2.tests.servers import WhenElectionDone
from raftframe.v2.tests.servers import WhenAllMessagesForwarded, WhenAllInMessagesHandled
from raftframe.v2.tests.servers import WhenInMessageCount
from raftframe.v2.tests.servers import PausingCluster, cluster_maker


async def test_stepwise_election_1(cluster_maker):
    """This test is mainly for the purpose of testing the test support
        features implemented in the PausingCluster, PausingServer and
        various Condition implementations. Other tests already proved
        the basic election process using more granular control
        methods, this does the same kind of thing but using the
        run_till_trigger model of controlling the code. It is still somewhat
        granular, and serves as a demo of how to build tests that can run 
        things, stop em, examine state, and continue
    """
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
    out1 = WhenMessageOut(RequestVoteMessage.get_code(),
                          message_target=uri_1, flush_when_done=False)
    ts_3.add_trigger(out1)
    out2 = WhenMessageOut(RequestVoteMessage.get_code(),
                          message_target=uri_2, flush_when_done=False)
    ts_3.add_trigger(out2)
    await ts_3.run_till_triggers()
    ts_3.clear_triggers()

    # Candidate is poised to send request for vote to other two servers
    # let the messages go out
    candidate = ts_3.hull.state
    logger.debug("Candidate posted vote requests for term %d", candidate.log.get_term())
    logger.debug("ts_1 term %d", ts_1.log.get_term())
    logger.debug("ts_2 term %d", ts_1.log.get_term())

    # let just these messages go
    ts_3.set_trigger(WhenAllMessagesForwarded())
    await ts_3.run_till_triggers()
    ts_3.clear_triggers()

    # Wait for the other services to send their vote responses back
    ts_1.set_trigger(WhenMessageIn(RequestVoteMessage.get_code()))
    ts_2.set_trigger(WhenMessageIn(RequestVoteMessage.get_code()))
    await ts_1.run_till_triggers()
    ts_1.clear_triggers()
    await ts_2.run_till_triggers()
    ts_2.clear_triggers()

    logger.debug("Followers have messages pending")
    ts_1.set_trigger(WhenAllInMessagesHandled())
    ts_2.set_trigger(WhenAllInMessagesHandled())
    await ts_1.run_till_triggers()
    await ts_2.run_till_triggers()

    logger.debug("Followers outgoing vote response messages pending")

    assert ts_1.hull.state.last_vote.sender == uri_3
    assert ts_2.hull.state.last_vote.sender == uri_3
    # Let all the messages fly until delivered
    await cluster.deliver_all_pending()
    assert ts_3.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_3
    assert ts_2.hull.state.leader_uri == uri_3

    logger.info("Stepwise paused election test completed")

async def test_run_to_election_1(cluster_maker):
    """This test is mainly for the purpose of testing the test support
        features implemented in the PausingCluster, PausingServer and
        various Condition implementations. This test shows how to use
        the least granular style of control, just allowing everything
        (except timers) proceed normally until the election is complete.
    """
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

    logger.info("-------- Initial election completion pause test completed starting reelection")
    # now have leader resign, by telling it to become follower
    await ts_3.hull.demote_and_handle(None)
    assert ts_3.hull.get_state_code() == "FOLLOWER"
    # simulate timeout on heartbeat on only one follower, so it should win
    await ts_2.hull.state.leader_lost()
    
    ts_1.set_trigger(WhenElectionDone())
    ts_2.set_trigger(WhenElectionDone())
    ts_3.set_trigger(WhenElectionDone())
        
    await asyncio.gather(ts_1.run_till_triggers(),
                         ts_2.run_till_triggers(),
                         ts_3.run_till_triggers())
    
    ts_1.clear_triggers()
    ts_2.clear_triggers()
    ts_3.clear_triggers()

    assert ts_2.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_2
    assert ts_3.hull.state.leader_uri == uri_2
    logger.info("-------- Re-election test done")

    
async def test_election_timeout_1(cluster_maker):
    cluster = cluster_maker(3)
    config = cluster.build_cluster_config(election_timeout_min=0.01,
                                          election_timeout_max=0.011)
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

    logger.info("-------- Initial election completion, starting reelection")
    # now have leader resign, by telling it to become follower
    await ts_3.hull.demote_and_handle(None)
    assert ts_3.hull.get_state_code() == "FOLLOWER"
    # simulate timeout on heartbeat on only one follower, so it should win
    await ts_2.hull.state.leader_lost()

    # now delay for more than the timeout, should start new election with new term
    old_term = ts_2.hull.log.get_term()
    await asyncio.sleep(0.015)
    new_term = ts_2.hull.log.get_term()
    assert new_term == old_term + 1

    # now it should just finish, everybody should know what to do
    # with messages rendered irrelevant by restart
    ts_1.set_trigger(WhenElectionDone())
    ts_2.set_trigger(WhenElectionDone())
    ts_3.set_trigger(WhenElectionDone())
        
    await asyncio.gather(ts_1.run_till_triggers(),
                         ts_2.run_till_triggers(),
                         ts_3.run_till_triggers())
    
    ts_1.clear_triggers()
    ts_2.clear_triggers()
    ts_3.clear_triggers()

    assert ts_2.hull.get_state_code() == "LEADER"
    assert ts_1.hull.state.leader_uri == uri_2
    assert ts_3.hull.state.leader_uri == uri_2
    logger.info("-------- Re-election timeout test done")


    # do the same sequence, only this time set the stopped flag on the
    # candidate to make sure the election timeout does not start another
    # election
    assert ts_2.hull.demote_and_handle()
    await ts_1.hull.state.leader_lost()
    assert ts_1.hull.get_state_code() == "CANDIDATE"
    # Set the stopped flag to prevent timeout from restarting election
    # don't call stop(), it cancels the timeout
    ts_1.hull.state.stopped = True
    # now delay for more than the timeout, should start new election with new term
    old_term = ts_1.hull.get_term()
    assert ts_1.hull.state_async_handle is not None
    await asyncio.sleep(0.015)
    assert ts_1.hull.get_state_code() == "CANDIDATE"
    new_term = ts_1.hull.get_term()
    assert new_term == old_term

    
    logger.info("-------- Election restart on timeout prevention test passed")

