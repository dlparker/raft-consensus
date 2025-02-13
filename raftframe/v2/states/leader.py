import logging
import asyncio
import time
import json
import traceback
from dataclasses import dataclass
from typing import Dict, List, Any
from enum import Enum
from raftframe.v2.states.base_state import StateCode, BaseState
from raftframe.v2.log.log_api import LogRec
from raftframe.v2.messages.append_entries import AppendEntriesMessage

class PushStatusCode(str, Enum):
    sent = "SENT"
    acked = "ACKED"

@dataclass
class PushRecord:
    status: PushStatusCode
    result: None
    
@dataclass
class CommandTracker:
    term: int
    prevIndex: int
    commands: List[str]
    finished: Any
    pushes: Dict[str, PushRecord]

    
class Leader(BaseState):

    def __init__(self, hull, term):
        super().__init__(hull, StateCode.leader)
        self.last_broadcast_time = 0
        self.pending_command = None
        self.old_commands = dict()  # commands that have not yet got all response, but are committed

    async def start(self):
        await super().start()
        await self.run_after(self.hull.get_heartbeat_period(), self.send_heartbeats)
        await self.send_heartbeats()

    async def apply_command(self, command, timeout=1.0):
        self.logger.info("%s requested command sequence", self.hull.get_my_uri())
        if self.pending_command:
            self.logger.info("%s waiting for completion of pending command", self.hull.get_my_uri())
            start_time = time.time()
            while self.pending_command and time.time() - start_time < timeout:
                await asyncio.sleep(0.001)
        if self.pending_command:
            msg = f'command already pending not completed in {timeout} seconds, command was'
            msg += f" {self.pending_command.str}"
            raise Exception(msg)
        self.logger.info("%s starting command sequence for index %d", self.hull.get_my_uri(),
                         self.log.get_last_index())
        consensus_condition = asyncio.Condition()
        self.pending_command = CommandTracker(term=self.log.get_term(),
                                              prevIndex=self.log.get_last_index(),
                                              finished=consensus_condition,
                                              pushes=dict(),
                                              commands=[command,])

        run_result = None
        await self.send_entries()
        async def done_check():
            if self.pending_command is None:
                return
            await asyncio.sleep(0.0001)
        try:
            await asyncio.wait_for(asyncio.create_task(done_check()), timeout=timeout)
        except asyncio.TimeoutError:
            msg = f'Requested command sequence not completed in {timeout} seconds'
            raise Exception(msg)
        try:
            self.logger.info("%s applying command committed at index %d", self.hull.get_my_uri(),
                             self.log.get_last_index())
            processor = self.hull.get_processor()
            result,error = await processor.process_command(command)
        except Exception as e:
            error = traceback.format_exc()
            result = None
        run_result = dict(command=command,
                          result=result,
                          error=error)
        new_rec = LogRec(term=self.log.get_term(),
                         user_data=json.dumps(run_result))
        self.log.append([new_rec,])
        return result, error
        
    async def send_heartbeats(self):
        silent_time = time.time() - self.last_broadcast_time
        remaining_time = self.hull.get_heartbeat_period() - silent_time
        if  remaining_time > 0:
            self.logger.debug("%s resched heartbeats time left %f", self.hull.get_my_uri, remaining_time)
            await self.run_after(remaining_time, self.send_heartbeats)
            return
        if self.pending_command:
            wait_time = self.hull.get_heartbeat_period() / 50.0
            self.logger.debug("%s pending command, resched heartbeats time left %f",
                              self.hull.get_my_uri, wait_time)
            await self.run_after(wait_time, self.send_heartbeats)
            return
        for nid in self.hull.get_cluster_node_ids():
            if nid == self.hull.get_my_uri():
                continue
            message = AppendEntriesMessage(sender=self.hull.get_my_uri(),
                                           receiver=nid,
                                           term=self.log.get_term(),
                                           entries=[],
                                           prevLogTerm=self.log.get_term(),
                                           prevLogIndex=self.log.get_last_index())
            self.logger.debug("%s sending heartbeat to %s", message.sender, message.receiver)
            await self.hull.send_message(message)
        self.last_broadcast_time = time.time()
        
    async def send_entries(self):
        self.command_finished = False
        tracker = self.pending_command
        for nid in self.hull.get_cluster_node_ids():
            if nid == self.hull.get_my_uri():
                continue
            tracker.pushes[nid] = PushRecord(status=PushStatusCode.sent, result=None)
            message = AppendEntriesMessage(sender=self.hull.get_my_uri(),
                                           receiver=nid,
                                           term=self.log.get_term(),
                                           entries=tracker.commands,
                                           prevLogTerm=self.log.get_term(),
                                           prevLogIndex=self.log.get_last_index())
            self.logger.info("sending %s", message)
            await self.hull.send_message(message)
        self.last_broadcast_time = time.time()
        
    async def catch_follower_up(self, message):
        if message.prevLogIndex == message.myPrevLogIndex:
            return
        # get the first log record they are missing, send that one
        rec = self.log.read(message.myPrevLogIndex + 1)
        command = json.loads(rec.user_data)['command']
        entries = [command,]
        message = AppendEntriesMessage(sender=self.hull.get_my_uri(),
                                       receiver=message.sender,
                                       term=self.log.get_term(),
                                       entries=entries,
                                       prevLogTerm=self.log.get_term(),
                                       prevLogIndex=self.log.get_last_index())
        self.logger.info("sending catchup %s", message)
        await self.hull.send_message(message)
        
    async def on_append_entries_response(self, message):
        current = True
        if self.pending_command is None:
            current = False
        elif self.pending_command.prevIndex > message.prevLogIndex:
            current = False
        if not current:
            # maybe some old push that hasn't recorded all replies yet
            old_rec = self.old_commands.get(message.prevLogIndex, None)
            if not old_rec:
                # prolly just a heartbeat, but check to see if catchup needed
                if message.prevLogIndex > message.myPrevLogIndex:
                    await self.catch_follower_up(message)
                return
            tracker = old_rec
        else:
            tracker = self.pending_command
        if message.prevLogIndex != tracker.prevIndex:
            self.logger.error("%s got append entries response that can't be identifed", self.hull.get_my_uri())
            return
        tracker.pushes[message.sender] = "acked"
        acked = 0
        for nid in self.hull.get_cluster_node_ids():
            if nid == self.hull.get_my_uri():
                continue
            if tracker.pushes[nid] == "acked":
                acked += 1
        if current:
            if acked  > len(tracker.pushes) / 2:
                self.logger.info('%s got consensus on index %d, applying command', self.hull.get_my_uri(),
                                 message.prevLogIndex)
                if tracker.finished:
                    # current state is "committed" as defined in raft paper, command can
                    # be applied
                    self.logger.debug("%s notify", self.hull.get_my_uri())
                    self.pending_command = None
        else:
            # this is an old one, remove it if last reply
            if acked == len(tracker.pushes):
                del self.old_commands[tracker.prevIndex]
        
    async def term_expired(self, message):
        self.log.set_term(message.term)
        await self.hull.demote_and_handle(message)
        # don't reprocess message
        return None





