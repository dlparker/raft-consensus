import asyncio
import logging
import time
import pytest
from raftframe.v2.hull.hull_config import ClusterConfig, LocalConfig
from raftframe.v2.hull.hull import Hull
from raftframe.v2.messages.request_vote import RequestVoteMessage,RequestVoteResponseMessage
from raftframe.v2.messages.append_entries import AppendEntriesMessage, AppendResponseMessage
from dev_tools.memory_log_v2 import MemoryLog
from raftframe.v2.hull.api import PilotAPI

HEARTBEAT_PERIOD=1000 # must be less than leader_lost_timeout
LEADER_LOST_TIMEOUT=2000
ELECTION_TIMEOUT_MIN=0.15
ELECTION_TIMEOUT_MAX=0.350

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


class PauseCondition:

    async def is_met(self, server):
        return False

class WhenMessageOut(PauseCondition):
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

    async def is_met(self, server):
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
    
class WhenMessageIn(PauseCondition):
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
    
    async def is_met(self, server):
        done = False
        for message in server.in_messages:
            if message.get_code() == self.message_code:
                if self.message_sender is None:
                    done = True
                if self.message_sender == message.sender:
                    done = True
        return done
    
class WhenInMessageCount(PauseCondition):
    # Whenn a particular message has been transported
    # from a different server and placed in the input
    # pending buffer of this server a number of times.
    # Until the count is reached, messages will be processed,
    # then the last on will be held in the input queue.
    # If this is a problem follow the wait for this condition
    # with server.do_next_in_msg()

    def __init__(self, message_code, goal):
        self.message_code = message_code
        self.goal = goal
        self.captured = []
        self.logged_done = False

    def __repr__(self):
        msg = f"{self.__class__.__name__} {self.message_code} {self.goal}"
        return msg
    
    async def is_met(self, server):
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
    
    
class WhenAllMessagesForwarded(PauseCondition):
    # When the server has forwarded (i.e. transported) all
    # of its pending output messages to the other servers,
    # where they sit in the input queues.

    def __repr__(self):
        msg = f"{self.__class__.__name__}"
        return msg
    
    async def is_met(self, server):
        if len(server.out_messages) > 0:
            return False
        return True
    
class WhenAllInMessagesHandled(PauseCondition):
    # When the server has processed all the messages
    # in the input queue, submitting them to the raft
    # code for processing.

    def __repr__(self):
        msg = f"{self.__class__.__name__}"
        return msg
    
    async def is_met(self, server):
        if len(server.in_messages) > 0:
            return False
        return True
    
class WhenIsLeader(PauseCondition):
    # When the server has won the election and
    # knows it.
    def __repr__(self):
        msg = f"{self.__class__.__name__}"
        return msg
    
    async def is_met(self, server):
        if server.hull.get_state_code() == "LEADER":
            return True
        return False
    
class WhenHasLeader(PauseCondition):
    # When the server started following specified leader
    def __init__(self, leader_uri):
        self.leader_uri = leader_uri

    async def __repr__(self):
        msg = f"{self.__class__.__name__} leader={self.leader_uri}"
        return msg
        
    async def is_met(self, server):
        if server.hull.get_state_code() != "FOLLOWER":
            return False
        if server.hull.state.leader_uri == self.leader_uri:
            return True
        return False
    
class ConditionSet:

    def __init__(self, conditions=None, mode="and"):
        if conditions is None:
            conditions = []
        self.conditions = conditions
        self.mode = mode

    def add_condition(self, condition):
        self.conditions.append(condition)
        
class ConditionSetSet:
    
    def __init__(self, sets=None, mode="and"):
        if sets is None:
            sets = []
        self.sets = sets
        self.mode = mode

    def add_set(self, c_set):
        self.sets.append(c_set)
        

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
        self.cond_set = None
        self.cond_set_set = None
        self.condition = None

    def set_configs(self, local_config, cluster_config):
        self.cluster_config = cluster_config
        self.local_config = local_config
        self.hull = Hull(self.cluster_config, self.local_config, self)
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
            handle =  hull.state.async_handle
            await hull.state.stop()
            if handle:
                self.logger.debug('after %s %s stop, handle.cancelled() says %s',
                                 hull.state, hull.get_my_uri(), handle.cancelled())
            
        self.hull = None
        del hull

    def clear_conditions(self, hard=False):
        self.condition = None
        self.cond_set = None
        self.cond_set_set = None

    def set_condition(self, condition):
        if self.condition is not None:
            raise Exception('this is for single condition operation, already set')
        if self.cond_set is not None:
            raise Exception('only one condition mode allowed, already have single set')
        if self.cond_set_set is not None:
            raise Exception('only one condition mode allowed, already have multiple sets')
        self.condition = condition
        
    def add_condition(self, condition):
        if self.condition is not None:
            raise Exception('only one condition mode allowed, already have single')
        if self.cond_set_set is not None:
            raise Exception('only one condition mode allowed, already have multiple sets')
        if self.cond_set is None:
            self.cond_set = ConditionSet(mode="and")
        self.cond_set.add_condition(condition)
        
    def add_condition_set(self, condition_set):
        if self.condition is not None:
            raise Exception('only one condition mode allowed, already have single')
        if self.cond_set is None:
            raise Exception('only one condition mode allowed, already have single set')
        if self.cond_set_set is None:
            self.cond_set_set = ConditionSetSet(mode="and")
        self.cond_set_set.add_set(condition_set)

    async def run_till_conditions(self, timeout=1):
        start_time = time.time()
        done = False
        while not done and time.time() - start_time < timeout:
            if self.condition is not None:
                if await self.condition.is_met(self):
                    self.logger.debug(f"Condition {self.condition} met, run done")
                    done = True
                    break
            elif self.cond_set is not None:
                for_set = 0
                for cond in self.cond_set.conditions:
                    is_met = await cond.is_met(self)
                    if not is_met:
                        continue
                    for_set += 1
                    if self.cond_set.mode == "or":
                        self.logger.debug(f"Condition {cond} met, run done (or)")
                        done = True
                        break
                    else:
                        if for_set == len(self.cond_set.conditions):
                            self.logger.debug(f"Condition {cond} met, all met")
                            done = True
                            break
            elif self.cond_set_set is not None:
                sets_done = 0
                for cond_set in self.cond_set_set.sets:
                    for_set = 0
                    for cond in self.cond_set.conditions:
                        if not await cond.is_met(self):
                            continue
                        for_set += 1
                        if self.cond_set.mode == "or":
                            sets_done += 1
                            break
                        else:
                            if for_set == len(self.cond_set.conditions):
                                sets_done += 1
                    if self.cond_set_set.mode == "or" and sets_done > 0:
                        done = True
                        break
                    if sets_done == len(self.cond_set_set.sets):
                        done = True
            if not done:
                msg = await self.do_next_out_msg()
                if not msg:
                    msg = await self.do_next_in_msg()
                if not msg:
                    await asyncio.sleep(0.00001)
        if not done:
            raise Exception('timeout waiting for conditions')
        return # all conditions met as required by mode flags, so pause ops
    
class PausingCluster:

    def __init__(self, node_count, suffix=0):
        self.node_uris = []
        self.nodes = dict()
        self.logger = logging.getLogger(__name__)
        self.auto_comms_flag = False
        self.auto_comms_condition = asyncio.Condition()
        for i in range(node_count):
            nid = i + 1
            uri = f"mcpy://{nid}/{suffix}"
            self.node_uris.append(uri)
            t1s = PausingServer(uri, self)
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
            node.set_configs(local_config, cc)

    async def start(self, only_these=None):
        for uri, node in self.nodes.items():
            await node.start()

    async def send_message(self, message):
        node  = self.nodes[message.receiver]
        await node.accept_in_msg(message)
        
    async def deliver_all_pending(self):
        any = True
        # want to bounce around, not deliver each ts completely
        while any:
            any = False
            for uri, node in self.nodes.items():
                if len(node.in_messages) > 0:
                    await node.do_next_in_msg()
                    any = True
                if len(node.out_messages) > 0:
                    await node.do_next_out_msg()
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
