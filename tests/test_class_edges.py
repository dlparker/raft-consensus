#!/usr/bin/env python
import asyncio
import logging
import pytest
import time
from raftframe.hull.hull import Hull
from raftframe.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.messages.append_entries import AppendEntriesMessage, AppendResponseMessage

from tests.servers import PausingCluster, cluster_maker
from tests.servers import setup_logging

setup_logging()

async def test_bogus_pilot(cluster_maker):
    cluster = cluster_maker(3)
    config = cluster.build_cluster_config()
    cluster.set_configs(config)
    uri_1 = cluster.node_uris[0]
    ts_1 = cluster.nodes[uri_1]
    class BadPilot:
        pass
    with pytest.raises(Exception):
        Hull(ts_1.cluster_config, ts_1.local_config, BadPilot())
