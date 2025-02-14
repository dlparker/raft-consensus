#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
import traceback
from raftframe.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

from servers import WhenMessageOut, WhenMessageIn
from servers import WhenHasLogIndex
from servers import WhenInMessageCount, WhenElectionDone
from servers import WhenAllMessagesForwarded, WhenAllInMessagesHandled
from servers import PausingCluster, cluster_maker
from servers import setup_logging

setup_logging()

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
    logger.info('------------------------ Election done')
    await cluster.start_auto_comms()

    command_result = await ts_3.hull.apply_command("add 1")
    res1,err1 = command_result['result']
    assert res1 is not None
    assert err1 is None
    assert ts_1.operations.total == 1
    assert ts_2.operations.total == 1
    assert ts_3.operations.total == 1
    term = await ts_3.hull.log.get_term()
    index = await ts_3.hull.log.get_last_index()
    assert index == 1
    assert await ts_1.hull.log.get_term() == term
    assert await ts_1.hull.log.get_last_index() == index
    assert await ts_2.hull.log.get_term() == term
    assert await ts_2.hull.log.get_last_index() == index
    logger.debug('------------------------ Correct command done')
    
    await cluster.stop_auto_comms()
    command_result = await ts_1.hull.apply_command("add 1")
    assert command_result['redirect'] == uri_3
    logger.debug('------------------------ Correct redirect (follower) done')
    
    orig_term =  await ts_1.hull.get_term() 
    await ts_1.hull.state.leader_lost()
    assert ts_1.hull.get_state_code() == "CANDIDATE"
    command_result = await ts_1.hull.apply_command("add 1")
    assert command_result['retry'] is not None
    logger.debug('------------------------ Correct retry (candidate) done')
    # cleanup attempt to start election
    ts_1.clear_all_msgs()
    # set term back so it won't trigger leader to quit
    await ts_1.hull.get_log().set_term(orig_term)

    await ts_1.hull.demote_and_handle()
    await ts_3.hull.state.send_heartbeats()
    await cluster.deliver_all_pending()
    assert ts_1.hull.get_state_code() == "FOLLOWER"

    # Have a follower blow up, control the messages so that
    # leader only sends two append_entries, then check
    # for bad state on exploded follower. Then defuse
    # the exploder and trigger hearbeat. This should
    # result in replay of command to follower, which should
    # then catch up and the the correct state

    command_result = None
    async def command_runner(ts):
        nonlocal command_result
        logger.debug('running command in background')
        try:
            command_result = await ts.hull.apply_command("add 1")
        except Exception as e:
            logger.debug('running command in background error %s', traceback.format_exc())
            
        logger.debug('running command in background done')
    
    ts_1.operations.explode = True
    orig_index = await ts_3.hull.get_log().get_last_index()
    ts_3.set_trigger(WhenHasLogIndex(orig_index + 1))
    # also have to fiddle the heartbeat timer or the messages won't be sent
    loop = asyncio.get_event_loop()
    logger.debug('------------------------ Starting command runner ---')
    loop.create_task(command_runner(ts_3))
    logger.debug('------------------------ Starting run_till_triggers with others ---')
    await ts_3.run_till_triggers(free_others=True)
    ts_3.clear_triggers()
    assert command_result is not None
    res1,err1 = command_result['result']
    assert res1 is not None
    assert err1 is None
    assert ts_1.operations.exploded == True

    assert ts_2.operations.total == 2
    assert ts_3.operations.total == 2
    assert ts_1.operations.total == 1

    # do it a couple more times so we can test that catch up function works
    # when follower is behind more than one record
    
    orig_index = await ts_3.hull.get_log().get_last_index()
    ts_3.set_trigger(WhenHasLogIndex(orig_index + 1))
    # also have to fiddle the heartbeat timer or the messages won't be sent
    loop = asyncio.get_event_loop()
    logger.debug('------------------------ Starting command runner ---')
    loop.create_task(command_runner(ts_3))
    logger.debug('------------------------ Starting run_till_triggers with others ---')
    await ts_3.run_till_triggers(free_others=True)
    ts_3.clear_triggers()
    assert command_result is not None
    res1,err1 = command_result['result']

    assert ts_2.operations.total == 3
    assert ts_3.operations.total == 3
    assert ts_1.operations.total == 1

    orig_index = await ts_3.hull.get_log().get_last_index()
    ts_3.set_trigger(WhenHasLogIndex(orig_index + 1))
    # also have to fiddle the heartbeat timer or the messages won't be sent
    loop = asyncio.get_event_loop()
    logger.debug('------------------------ Starting command runner ---')
    loop.create_task(command_runner(ts_3))
    logger.debug('------------------------ Starting run_till_triggers with others ---')
    await ts_3.run_till_triggers(free_others=True)
    ts_3.clear_triggers()
    assert command_result is not None
    res1,err1 = command_result['result']

    assert ts_2.operations.total == 4
    assert ts_3.operations.total == 4
    assert ts_1.operations.total == 1

    # now send heartbeats and ensure that exploded follower catches up
    ts_1.operations.explode = False
    ts_3.hull.state.last_broadcast_time = 0
    logger.debug('---------Sending heartbeat and starting run_till_triggers with others ---')
    await ts_3.hull.state.send_heartbeats()
    cur_index = await ts_3.hull.get_log().get_last_index()
    ts_1.set_trigger(WhenHasLogIndex(cur_index))
    await ts_1.run_till_triggers(free_others=True)
    assert ts_1.operations.total == 4
