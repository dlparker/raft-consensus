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

async def test_election_vote_once_1(cluster_maker):
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

    logger.info("-------- Initial election completion, starting messed up re-election")

    # now have leader resign, by telling it to become follower
    await ts_3.hull.demote_and_handle(None)
    assert ts_3.hull.get_state_code() == "FOLLOWER"
    # simulate timeout on heartbeat on two followers causing
    # two candidates to try election in same term
    await ts_2.hull.state.leader_lost()
    await ts_3.hull.state.leader_lost()

    # now let the remainging follower see only the vote request from 
    # one candidate, so let one of them send their two messages
    msg1 = await ts_2.do_next_out_msg()
    msg2 = await ts_2.do_next_out_msg()
    # just to make sure our logic is working
    assert msg1.receiver == uri_1 or msg2.receiver == uri_1

    # now let the follower cast the vote, which should be true
    await ts_1.do_next_in_msg()
    vote_yes_msg = await ts_1.do_next_out_msg()
    assert vote_yes_msg.vote == True
    await ts_2.do_next_in_msg()

    # now let the other candidate send requests
    msg3 = await ts_3.do_next_out_msg()
    msg4 = await ts_3.do_next_out_msg()
    # just to make sure our logic is working
    assert msg3.receiver == uri_1 or msg4.receiver == uri_1

    # now let the follower cast the vote, which should be false
    await ts_1.do_next_in_msg()
    vote_no_msg = await ts_1.do_next_out_msg()
    assert vote_no_msg.vote == False
    await ts_3.do_next_in_msg()

    # now it should just finish, everybody should know what to do

    logger.info("-------- allowing election to continue ---")
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
    logger.info("-------- Re-election vote once test complete ---")


async def test_election_candidate_too_slow_1(cluster_maker):
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

    logger.info("-------- Initial election completion, starting messed up re-election")

    # now have leader resign, by telling it to become follower
    await ts_3.hull.demote_and_handle(None)
    assert ts_3.hull.get_state_code() == "FOLLOWER"
    # simulate timeout on heartbeat on two followers causing
    # two candidates to try election but fiddle one of them
    # to have a higher term
    await ts_2.hull.state.leader_lost()
    term = ts_3.hull.log.get_term()
    ts_3.hull.log.set_term(term + 1)
    await ts_3.hull.state.leader_lost()

    # Let the low term one send vote request first,
    # then before it receives any replies let the second
    # one send requests.
    msg1 = await ts_2.do_next_out_msg()
    msg2 = await ts_2.do_next_out_msg()
    
    logger.debug("-------- Letting first candidate win follower's vote")
    # now let the follower cast the vote, which should be true
    await ts_1.do_next_in_msg()
    vote_yes_msg_1 = await ts_1.do_next_out_msg()
    assert vote_yes_msg_1.vote == True
    # Don't let ts_2 get response, we want it to stay a candidate
    assert ts_2.hull.get_state_code() == "CANDIDATE"
    logger.debug("-------- First candidate is now has yes vote pending ---")
    # save the pending yes vote and let the other candidate's votes in
    # instead
    saved_vote = ts_2.in_messages.pop(0)
    # now let the second candidate requests in to the first one.
    # which should accept the new candidate because of the higher
    # term. Have to let both requests go
    msg3 = await ts_3.do_next_out_msg()
    msg4 = await ts_3.do_next_out_msg()

    # this should trigger old term candidate to resign
    # in favor of new term candidate
    await ts_2.do_next_in_msg()
    assert ts_2.hull.get_state_code() == "FOLLOWER"
    logger.debug("-------- First candidate has accepted second candidate and resigned ")

    # Push the out of date vote back on to what used
    # to be the old candidate, just for completeness.
    # Might uncover a regression some day
    # let everbody go and check the results
    ts_2.in_messages.append(saved_vote)

    # now let everbody run and make sure election concludes
    # with second candidate elected
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
    
