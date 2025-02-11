
from pathlib import Path
import asyncio
import logging
import pytest
import time
from raftframe.v2.log.sqlite_log import SqliteLog
from raftframe.v2.hull.hull_config import LiveConfig, ClusterConfig, LocalConfig
from raftframe.v2.hull.hull import Hull
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage
from dev_tools.memory_log_v2 import MemoryLog
from raftframe.v2.hull.api import PilotAPI


HEARTBEAT_PERIOD=0.03 # must be less than leader_lost_timeout
LEADER_LOST_TIMEOUT=0.1
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

class simpleOps():
    total = 0
    async def process_command(self, command):
        logger = logging.getLogger(__name__)
        error = None
        result = None
        op, operand = command.split()
        if op not in ['add', 'sub']:
            error = "invalid command"
            logger.error("invalid command %s provided", op)
            return None, error
        if op == "add":
            self.total += int(operand)
        elif op == "sub":
            self.total -= int(operand)
        result = self.total
        logger.error("command %s returning %s no error", command, result)
        return result, None
        
class T1Cluster:

    def __init__(self, node_count):
        self.node_uris = []
        self.nodes = dict()
        self.logger = logging.getLogger(__name__)
        self.auto_comms_flag = False
        self.auto_comms_condition = asyncio.Condition()
        for i in range(node_count):
            nid = i + 1
            uri = f"mcpy://{nid}"
            self.node_uris.append(uri)
            t1s = T1Server(uri, self)
            self.nodes[uri] = t1s
        assert len(self.node_uris) == node_count

    def set_configs(self):
        for uri, node in self.nodes.items():
            # in real code you'd have only on cluster config in
            # something like a cluster manager, but in test
            # code we sometimes want to change something
            # in it for only some of the servers, not all,
            # so each gets its own copy
            cc = ClusterConfig(node_uris=self.node_uris,
                               heartbeat_period=HEARTBEAT_PERIOD,
                               leader_lost_timeout=LEADER_LOST_TIMEOUT,
                               election_timeout_min=ELECTION_TIMEOUT_MIN,
                               election_timeout_max=ELECTION_TIMEOUT_MAX,)
                           
            local_config = LocalConfig(uri=uri,
                                       working_dir='/tmp/',
                                       )
            data_log = MemoryLog()
            live_config = LiveConfig(cluster=cc,
                                     log=data_log,
                                     local=local_config)
            node.set_config(live_config)

    async def start(self, only_these=None):
        for uri, node in self.nodes.items():
            await node.start()
            
    async def response_sender(self, target_uri, in_msg, reply_msg):
        target = self.nodes[target_uri]
        target.in_messages.append(reply_msg)
        self.logger.debug("queueing reply %s", reply_msg)
        if self.auto_comms_flag:
            async with self.auto_comms_condition:
                self.auto_comms_condition.notify_all()

    async def message_sender(self, target_uri, msg):
        target = self.nodes[target_uri]
        target.in_messages.append(msg)
        self.logger.debug("queueing message %s", msg)
        if self.auto_comms_flag:
            async with self.auto_comms_condition:
                self.auto_comms_condition.notify_all()

    async def deliver_all_pending(self):
        any = True
        # want to bounce around, not deliver each ts completely
        while any:
            any = False
            for uri, node in self.nodes.items():
                if len(node.in_messages) > 0:
                    await node.do_next_msg()
                    any = True

    async def auto_comms_runner(self):
        while self.auto_comms_flag:
            await self.deliver_all_pending()
            async with self.auto_comms_condition:
                await self.auto_comms_condition.wait()
            
    async def start_auto_comms(self):
        self.auto_comms_flag = True
        loop = asyncio.get_event_loop()
        loop.call_soon(lambda: loop.create_task(self.auto_comms_runner()))
        
    async def cleanup(self):
        for uri, node in self.nodes.items():
            await node.cleanup()
        # lose references to everything
        self.nodes = {}
        self.node_uris = []

            
class T1Server(PilotAPI):

    def __init__(self, uri, cluster):
        self.uri = uri
        self.cluster = cluster
        self.live_config = None
        self.hull = None
        self.in_messages = []
        self.logger = logging.getLogger(__name__)

    def set_config(self, live_config):
        self.live_config = live_config
        self.hull = Hull(self.live_config, self)
        self.operations = simpleOps()

    # Part of PilotAPI
    async def process_command(self, command):
        return await self.operations.process_command(command)
        
    # Part of PilotAPI
    async def send_message(self, target, msg):
        await self.cluster.message_sender(target, msg)

    # Part of PilotAPI
    async def send_response(self, target, in_msg, reply):
        await self.cluster.response_sender(target, in_msg, reply)
                
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


    
    
    
