import asyncio
import logging
import time
import pytest
import functools
import dataclasses
from collections import defaultdict
from raftframe.v2.hull.hull_config import ClusterConfig, LocalConfig
from raftframe.v2.hull.hull import Hull
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage
from raftframe.v2.messages.base_message import BaseMessage
from dev_tools.memory_log_v2 import MemoryLog
from raftframe.v2.hull.api import PilotAPI

@pytest.fixture
async def cluster_maker():
    the_cluster = None

    def make_cluster(*args, **kwargs):
        nonlocal the_cluster
        the_cluster =  PausingCluster(*args, **kwargs)
        return the_cluster
    yield make_cluster
    if the_cluster is not None:
        await the_cluster.cleanup()
    
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


class PauseTrigger:

    async def is_tripped(self, server):
        return False

class WhenMessageOut(PauseTrigger):
    # When a particular message have been sent
    # by the raft code, and is waiting to be transported
    # to the receiver. You can just check the message
    # type, or require that type and a specific target receiver.
    # If you don't care about inspecting the message before it
    # is transported to the target server, leave the flush_when_done
    # flag set to True, otherwise set if false and then arrange for
    # transport after inspecting.
    def __init__(self, message_code, message_target=None, flush_when_done=True):
        self.message_code = message_code
        self.message_target = message_target
        self.flush_when_done = flush_when_done

    def __repr__(self):
        msg = f"{self.__class__.__name__} {self.message_code} {self.message_target}"
        return msg

    async def is_tripped(self, server):
        done = False
        for message in server.out_messages:
            if message.get_code() == self.message_code:
                if self.message_target is None:
                    done = True
                elif self.message_target == message.receiver:
                    done = True
        if done and self.flush_when_done:
            await server.flush_one_out_message(message)            
        return done
    
class WhenMessageIn(PauseTrigger):
    # Whenn a particular message have been transported
    # from a different server and placed in the input
    # pending buffer of this server. The message
    # in question has not yet been delivered to the
    # raft code. You can just check the message
    # type, or require that type and a specific sender
    def __init__(self, message_code, message_sender=None):
        self.message_code = message_code
        self.message_sender = message_sender

    def __repr__(self):
        msg = f"{self.__class__.__name__} {self.message_code} {self.message_sender}"
        return msg
    
    async def is_tripped(self, server):
        done = False
        for message in server.in_messages:
            if message.get_code() == self.message_code:
                if self.message_sender is None:
                    done = True
                if self.message_sender == message.sender:
                    done = True
        return done
    
class WhenInMessageCount(PauseTrigger):
    # Whenn a particular message has been transported
    # from a different server and placed in the input
    # pending buffer of this server a number of times.
    # Until the count is reached, messages will be processed,
    # then the last on will be held in the input queue.
    # If this is a problem follow the wait for this trigger
    # with server.do_next_in_msg()

    def __init__(self, message_code, goal):
        self.message_code = message_code
        self.goal = goal
        self.captured = []
        self.logged_done = False

    def __repr__(self):
        msg = f"{self.__class__.__name__} {self.message_code} {self.goal}"
        return msg
    
    async def is_tripped(self, server):
        logger = logging.getLogger(__name__)
        for message in server.in_messages:
            if message.get_code() == self.message_code:
                if message not in self.captured:
                    self.captured.append(message)
                    logger.debug("%s captured = %s", self, self.captured)
        if len(self.captured) == self.goal:
            if not self.logged_done:
                logger.debug("%s satisfied ", self)
                self.logged_done = True
            return True
        else:
            return False
    
    
class WhenAllMessagesForwarded(PauseTrigger):
    # When the server has forwarded (i.e. transported) all
    # of its pending output messages to the other servers,
    # where they sit in the input queues.

    def __repr__(self):
        msg = f"{self.__class__.__name__}"
        return msg
    
    async def is_tripped(self, server):
        if len(server.out_messages) > 0:
            return False
        return True
    
class WhenAllInMessagesHandled(PauseTrigger):
    # When the server has processed all the messages
    # in the input queue, submitting them to the raft
    # code for processing.

    def __repr__(self):
        msg = f"{self.__class__.__name__}"
        return msg
    
    async def is_tripped(self, server):
        if len(server.in_messages) > 0:
            return False
        return True
    
class WhenIsLeader(PauseTrigger):
    # When the server has won the election and
    # knows it.
    def __repr__(self):
        msg = f"{self.__class__.__name__}"
        return msg
    
    async def is_tripped(self, server):
        if server.hull.get_state_code() == "LEADER":
            return True
        return False
    
class WhenHasLeader(PauseTrigger):
    # When the server started following specified leader
    def __init__(self, leader_uri):
        self.leader_uri = leader_uri

    def __repr__(self):
        msg = f"{self.__class__.__name__} leader={self.leader_uri}"
        return msg
        
    async def is_tripped(self, server):
        if server.hull.get_state_code() != "FOLLOWER":
            return False
        if server.hull.state.leader_uri == self.leader_uri:
            return True
        return False
    
class WhenElectionDone(PauseTrigger):
    # Examine whole cluster to make sure we are in the
    # post election quiet period

    def __init__(self):
        self.announced = defaultdict(dict)
        
    def __repr__(self):
        msg = f"{self.__class__.__name__}"
        return msg
        
    async def is_tripped(self, server):
        logger = logging.getLogger(__name__)
        quiet = []
        have_leader = False
        for uri, node in server.cluster.nodes.items():
            if node.hull.get_state_code() == "LEADER":
                have_leader = True
                rec = self.announced[uri]
                if "is_leader" not in rec:
                    rec['is_leader'] = True
                    logger.debug('%s is now leader', uri)
            if len(node.in_messages) == 0 and len(node.out_messages) == 0:
                quiet.append(uri)
                rec = self.announced[uri]
                if "is_quiet" not in rec:
                    rec['is_quiet'] = True
                    logger.debug('%s is now quiet, total quiet == %d', uri, len(quiet))
        if have_leader and len(quiet) == len(server.cluster.nodes):
            return True
        return False
    
class TriggerSet:

    def __init__(self, triggers=None, mode="and", name=None):
        if triggers is None:
            triggers = []
        self.triggers = triggers
        self.mode = mode
        if name is None:
            name = f"Set-[str(cond) for cond in triggers]"
        self.name = name

    def __repr__(self):
        return self.name

    def add_trigger(self, trigger):
        self.triggers.append(trigger)

    async def is_tripped(self, server):
        logger = logging.getLogger(__name__)
        for_set = 0
        for cond in self.triggers:
            is_tripped = await cond.is_tripped(server)
            if not is_tripped:
                if self.mode == "and":
                    return False
            for_set += 1
            if self.mode == "or":
                logger.debug(f"%s Trigger {cond} tripped, run done (or)", server.uri)
                return True
            if for_set == len(self.triggers):
                logger.debug(f"%s Trigger {cond} tripped, all tripped", server.uri)
                return True
        return False

class TestHull(Hull):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.break_on_message_code = None
        self.state_run_later_def = None
        self.timers_paused = False
        self.condition = asyncio.Condition()
        
    async def on_message(self, message):
        if self.break_on_message_code == message.get_code():
            breakpoint()
            print('here to catch break')
        result = await super().on_message(message)
        return result

    async def pause_timers(self):
        self.timers.paused = True
            
    async def release_timers(self):
        async with self.condition:
            self.timers.paused = True
            await self.condition.notify()
            
    async def state_after_runner(self, target):
        if self.state.stopped:
            return
        while self.timers_paused:
            async with self.condition:
                await self.condition.wait()
        await target()

    async def state_run_after(self, delay, target):
        self.state_run_later_def = dict(state_code=self.state.state_code,
                                        delay=delay, target=target)
        await super().state_run_after(delay, target)

class PausingServer(PilotAPI):

    def __init__(self, uri, cluster):
        self.uri = uri
        self.cluster = cluster
        self.cluster_config = None
        self.local_config = None
        self.hull = None
        self.in_messages = []
        self.out_messages = []
        self.logger = logging.getLogger(__name__)
        self.log = MemoryLog()
        self.trigger_set = None
        self.trigger = None
        self.break_on_message_code = None

    def set_configs(self, local_config, cluster_config):
        self.cluster_config = cluster_config
        self.local_config = local_config
        self.hull = TestHull(self.cluster_config, self.local_config, self)
        self.operations = simpleOps()

    # Part of PilotAPI
    def get_log(self):
        return self.log

    # Part of PilotAPI
    async def process_command(self, command):
        return await self.operations.process_command(command)
        
    # Part of PilotAPI
    async def send_message(self, target, msg):
        self.logger.debug("queueing out msg %s", msg)
        self.out_messages.append(msg) 

    # Part of PilotAPI
    async def send_response(self, target, in_msg, reply):
        self.logger.debug("queueing out reply %s", reply)
        self.out_messages.append(reply) 
        
    async def start(self):
        await self.hull.start()
        
    async def start_election(self):
        await self.hull.campaign()

    async def accept_in_msg(self, message):
        # called by cluster on behalf of sender
        self.logger.debug("queueing sent %s", message)
        self.in_messages.append(message)
        
    async def do_next_in_msg(self):
        if len(self.in_messages) == 0:
            return None
        msg = self.in_messages.pop(0)
        self.logger.debug("%s handling message %s", self.hull.get_my_uri(), msg)
        await self.hull.on_message(msg)
        return msg

    async def do_next_out_msg(self):
        if len(self.out_messages) == 0:
            return None
        msg = self.out_messages.pop(0)
        self.logger.debug("forwarding message %s", msg)
        await self.cluster.send_message(msg)
        return msg

    async def flush_one_out_message(self, message):
        if len(self.out_messages) == 0:
            return None
        new_list = []
        for msg in self.out_messages:
            if msg == message:
                self.logger.debug("FLUSH forwarding message %s", msg)
                await self.cluster.send_message(msg)
            else:
                new_list.append(msg)
        self.out_messages = new_list

    async def cleanup(self):
        hull = self.hull
        if hull.state:
            self.logger.debug('cleanup stopping %s %s', hull.state, hull.get_my_uri())
            handle =  hull.state_async_handle
            await hull.state.stop()
            if handle:
                self.logger.debug('after %s %s stop, handle.cancelled() says %s',
                                 hull.state, hull.get_my_uri(), handle.cancelled())
            
        self.hull = None
        del hull

    def clear_triggers(self):
        self.trigger = None
        self.trigger_set = None

    def set_trigger(self, trigger):
        if self.trigger is not None:
            raise Exception('this is for single trigger operation, already set')
        if self.trigger_set is not None:
            raise Exception('only one trigger mode allowed, already have single set')
        self.trigger = trigger
        
    def add_trigger(self, trigger):
        if self.trigger is not None:
            raise Exception('only one trigger mode allowed, already have single')
        if self.trigger_set is None:
            self.trigger_set = TriggerSet(mode="and")
        self.trigger_set.add_trigger(trigger)
        
    def add_trigger_set(self, trigger_set):
        if self.trigger is not None:
            raise Exception('only one trigger mode allowed, already have single')
        if self.trigger_set is None:
            raise Exception('only one trigger mode allowed, already have single set')
        self.trigger_set_set.add_set(trigger_set)

    async def run_till_triggers(self, timeout=1):
        start_time = time.time()
        done = False
        while not done and time.time() - start_time < timeout:
            if self.trigger is not None:
                if await self.trigger.is_tripped(self):
                    self.logger.debug(f"%s Trigger {self.trigger} tripped, run done", self.uri)
                    done = True
                    break
            elif self.trigger_set is not None:
                if await self.trigger_set.is_tripped(self):
                    self.logger.debug(f"%s TriggerSet {self.trigger_set} tripped, run done", self.uri)
                    done = True
                    break
            elif self.trigger_set_set is not None:
                if await self.trigger_set_set.is_tripped(self):
                    self.logger.debug(f"%s TriggerSetSet {self.trigger_set_set} tripped, run done", self.uri)
                    done = True
                    break
            if not done:
                msg = await self.do_next_out_msg()
                if not msg:
                    msg = await self.do_next_in_msg()
                if not msg:
                    await asyncio.sleep(0.00001)
        if not done:
            raise Exception(f'{self.uri} timeout waiting for triggers')
        return # all triggers tripped as required by mode flags, so pause ops
    
class PausingCluster:

    def __init__(self, node_count):
        self.node_uris = []
        self.nodes = dict()
        self.logger = logging.getLogger(__name__)
        self.auto_comms_flag = False
        self.auto_comms_condition = asyncio.Condition()
        self.async_handle = None
        for i in range(node_count):
            nid = i + 1
            uri = f"mcpy://{nid}"
            self.node_uris.append(uri)
            t1s = PausingServer(uri, self)
            self.nodes[uri] = t1s
        assert len(self.node_uris) == node_count

    def build_cluster_config(self, heartbeat_period=1000,
                             leader_lost_timeout=1000,
                             election_timeout_min=0.15,
                             election_timeout_max=0.35):
        
            cc = ClusterConfig(node_uris=self.node_uris,
                               heartbeat_period=heartbeat_period,
                               leader_lost_timeout=leader_lost_timeout,
                               election_timeout_min=election_timeout_min,
                               election_timeout_max=election_timeout_max,)
            return cc

    def set_configs(self, cluster_config=None):
        if cluster_config is None:
            cluster_config = self.build_cluster_config()
        for uri, node in self.nodes.items():
            # in real code you'd have only on cluster config in
            # something like a cluster manager, but in test
            # code we sometimes want to change something
            # in it for only some of the servers, not all,
            # so each gets its own copy
            cc = dataclasses.replace(cluster_config)
                           
            local_config = LocalConfig(uri=uri,
                                       working_dir='/tmp/',
                                       )
            node.set_configs(local_config, cc)

    async def start(self, only_these=None):
        for uri, node in self.nodes.items():
            await node.start()

    async def send_message(self, message):
        node  = self.nodes[message.receiver]
        await node.accept_in_msg(message)
        
    async def deliver_all_pending(self,  out_only=False):
        in_ledger = []
        out_ledger = []
        any = True
        # want to bounce around, not deliver each ts completely
        while any:
            any = False
            for uri, node in self.nodes.items():
                if len(node.in_messages) > 0 and not out_only:
                    msg = await node.do_next_in_msg()
                    in_ledger.append(msg)
                    any = True
                if len(node.out_messages) > 0:
                    msg = await node.do_next_out_msg()
                    out_ledger.append(msg)
                    any = True
        return in_ledger, out_ledger

    async def auto_comms_runner(self):
        while self.auto_comms_flag:
            await self.deliver_all_pending()
            async with self.auto_comms_condition:
                await self.auto_comms_condition.wait()
            
    async def start_auto_comms(self):
        self.auto_comms_flag = True
        loop = asyncio.get_event_loop()
        self.async_handle = loop.call_soon(lambda: loop.create_task(self.auto_comms_runner()))
        
    async def cleanup(self):
        for uri, node in self.nodes.items():
            await node.cleanup()
        # lose references to everything
        self.nodes = {}
        self.node_uris = []
        if self.async_handle:
            self.async_handle.cancel()
            self.async_handle = None
