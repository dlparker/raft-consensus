#!/usr/bin/env python
from pathlib import Path
import asyncio
from collections import deque
import logging
import pytest
from raftframe.v2.log.sqlite_log import SqliteLog
from raftframe.v2.hull.hull_config import LiveConfig, ClusterConfig, LocalConfig
from raftframe.v2.hull.hull import Hull
from dev_tools.memory_log import MemoryLog


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

class T1Cluster:

    def __init__(self, node_count):
        self.node_uris = []
        self.nodes = dict()
        for i in range(node_count):
            nid = i + 1
            uri = f"mcpy://{nid}"
            self.node_uris.append(uri)
            t1s = T1Server(uri, self)
            self.nodes[uri] = t1s

    def set_configs(self):
        cc = ClusterConfig(node_uris=self.node_uris)
        for uri, node in self.nodes.items():
            local_config = LocalConfig(uri=uri,
                                       working_dir='/tmp/')
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

    async def message_sender(self, msg):
        target = self.nodes[msg.receiver]
        target.in_messages.append(msg)
            
class T1Server:

    def __init__(self, uri, cluster):
        self.uri = uri
        self.cluster = cluster
        self.live_config = None
        self.hull = None
        self.in_messages = deque()

    def set_config(self, live_config):
        self.live_config = live_config
        self.hull = Hull(self.live_config)

    async def start(self):
        await self.hull.start()

    async def do_next_msg(self):
        msg = self.in_messages.popleft()
        if msg:
            await self.hull.on_message(msg)
        return msg
        
async def test_tasks_1():

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    logger.info("foo")
    loop = asyncio.get_running_loop()
    cluster = T1Cluster(3)
    cluster.set_configs()
    await cluster.start()
    for uri, t1s in cluster.nodes.items():
        assert t1s.hull.get_log().get_term() == 0
