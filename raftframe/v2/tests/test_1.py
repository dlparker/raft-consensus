#!/usr/bin/env python
from pathlib import Path
import asyncio
import logging
import pytest
import time
from raftframe.v2.log.sqlite_log import SqliteLog
from raftframe.v2.hull.hull_config import LiveConfig, ClusterConfig, LocalConfig
from raftframe.v2.hull.hull import Hull
from raftframe.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage, AppendResponseMessage
from dev_tools.memory_log import MemoryLog

logging.basicConfig(level=logging.DEBUG)

LEADER_LOST_TIMEOUT=0.01
ELECTION_TIMEOUT_MIN=0.15
ELECTION_TIMEOUT_MAX=0.350

@pytest.fixture
def create_log_db():
    sourcedir = Path(__file__).resolve().parent
    target_db_name = "discard_log_db.sqlite"
    target_db_path = Path(sourcedir, target_db_name)
    if target_db_path.exists():
        target_db_path.unlink()
    log_db = SqliteLog()
    log_db.start(sourcedir)
    return [log_db, sourcedir]

@pytest.fixture
@pytest.mark.asyncio
async def cluster_of_three():
    cluster = T1Cluster(3)
    cluster.set_configs()
    yield cluster
    await cluster.cleanup()
    loop = asyncio.get_event_loop()
    await tear_down(loop)
    
@pytest.fixture
@pytest.mark.asyncio
async def cluster_of_five():
    cluster = T1Cluster(5)
    cluster.set_configs()
    yield cluster
    await cluster.cleanup()
    loop = asyncio.get_event_loop()
    await tear_down(loop)
    
async def tear_down(event_loop):
    # Collect all tasks and cancel those that are not 'done'.                                       
    tasks = asyncio.all_tasks(event_loop)
    tasks = [t for t in tasks if not t.done()]
    for task in tasks:
        task.cancel()

    # Wait for all tasks to complete, ignoring any CancelledErrors                                  
    try:
        await asyncio.wait(tasks)
    except asyncio.exceptions.CancelledError:
        pass
    
class T1Cluster:

    def __init__(self, node_count):
        self.node_uris = []
        self.nodes = dict()
        self.logger = logging.getLogger(__name__)
        for i in range(node_count):
            nid = i + 1
            uri = f"mcpy://{nid}"
            self.node_uris.append(uri)
            t1s = T1Server(uri, self)
            self.nodes[uri] = t1s
        assert len(self.node_uris) == node_count

    def set_configs(self):
        cc = ClusterConfig(node_uris=self.node_uris)
        for uri, node in self.nodes.items():
            local_config = LocalConfig(uri=uri,
                                       working_dir='/tmp/',
                                       leader_lost_timeout=LEADER_LOST_TIMEOUT,
                                       election_timeout_min=ELECTION_TIMEOUT_MIN,
                                       election_timeout_max=ELECTION_TIMEOUT_MAX,
                                       )
            data_log = MemoryLog()
            live_config = LiveConfig(cluster=cc,
                                     log=data_log,
                                     local=local_config,
                                     message_sender=self.message_sender,
                                     response_sender=self.response_sender)
            node.set_config(live_config)

    async def start(self, only_these=None):
        for uri, node in self.nodes.items():
            await node.start()
            
    async def response_sender(self, in_msg, reply_msg):
        target = self.nodes[reply_msg.receiver]
        target.in_messages.append(reply_msg)
        self.logger.debug("queueing reply %s", reply_msg)

    async def message_sender(self, msg):
        target = self.nodes[msg.receiver]
        target.in_messages.append(msg)
        self.logger.debug("queueing message %s", msg)

    async def deliver_all_pending(self):
        any = True
        # want do bounce around, not deliver each ts completely
        while any:
            any = False
            for uri, node in self.nodes.items():
                if len(node.in_messages) > 0:
                    await node.do_next_msg()
                    any = True
        
    async def cleanup(self):
        for uri, node in self.nodes.items():
            await node.cleanup()
        # lose references to everything
        self.nodes = {}
        self.node_uris = []

            
class T1Server:

    def __init__(self, uri, cluster):
        self.uri = uri
        self.cluster = cluster
        self.live_config = None
        self.hull = None
        self.in_messages = []
        self.logger = logging.getLogger(__name__)

    def set_config(self, live_config):
        self.live_config = live_config
        self.hull = Hull(self.live_config)

    async def start(self):
        await self.hull.start()

    async def start_election(self):
        await self.hull.campaign()

    async def do_next_msg(self):
        msg = self.in_messages.pop(0)
        if msg:
            self.logger.debug("delivering message %s", msg)
            await self.hull.on_message(msg)
        return msg

    async def cleanup(self):
        hull = self.hull
        if hull.state:
            self.logger.debug('cleanup stopping %s %s', hull.state, hull.get_my_uri())
            handle =  hull.state.async_handle
            await hull.state.stop()
            if handle:
                self.logger.debug('after %s %s stop, handle.cancelled() says %s',
                                 hull.state, hull.get_my_uri(), handle.cancelled())
            
        self.hull = None
        del hull

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
    ts_1.hull.config.local.leader_lost_timeout = 1
    ts_2.hull.config.local.leader_lost_timeout = 1
    ts_3.hull.config.local.leader_lost_timeout = 0.001

    # ensure that ts_2 wins re-election
    ts_1.hull.config.local.election_timeout_min = 1
    ts_1.hull.config.local.election_timeout_max = 1.2
    ts_2.hull.config.local.election_timeout_min = 0.001
    ts_2.hull.config.local.election_timeout_max = 0.0011
    ts_3.hull.config.local.election_timeout_min = 1
    ts_3.hull.config.local.election_timeout_max = 1.2
    
    await cluster.start()
    # give ts_3 time to timeout and start campaign
    await asyncio.sleep(0.0015)
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
    
    

    
    
    
