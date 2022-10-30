import random
import time
import asyncio
from dataclasses import dataclass, asdict
import logging
import traceback

from raft.utils import task_logger
from raft.log.log_api import LogRec
from raft.messages.append_entries import AppendResponseMessage
from raft.messages.log_pull import LogPullMessage
from .base_state import Substate
from .voter import Voter

@dataclass
class StateDiff:

    local_is_empty: int
    local_index: int
    local_commit: int
    local_term: int
    local_prev_term: int
    leader_term: int
    leader_prev_term: int
    leader_commit: int
    leader_index: int
    leader_addr: tuple

    @property
    def same_term(self):
        return self.local_term == self.leader_term
    
    @property
    def same_index(self):
        return self.local_index == self.leader_index
    
    @property
    def same_commit(self):
        return self.local_commit == self.leader_commit
    
    @property
    def same_prev_term(self):
        return self.local_prev_term == self.leader_prev_term
    
    @property
    def local_term_none(self):
        return self.local_term == -1
    
    @property
    def local_commit_none(self):
        return self.local_commit == -1

    @property
    def local_index_none(self):
        return self.local_index == -1

    @property
    def leader_commit_none(self):
        return self.leader_commit == -1
    
    @property
    def leader_prev_term_none(self):
        return self.leader_prev_term == -1
    
    @property
    def leader_index_none(self):
        return self.leader_index == -1

    @property
    def in_sync(self):
        if self.same_commit and self.same_term and self.same_prev_term:
            return True
        return False
    
    @property
    def needed_records(self):
        if self.local_ahead:
            return None
        if self.same_index:
            return None
        if self.same_commit:
            return None
        if self.leader_commit < self.leader_index:
            last = self.leader_commit
        else:
            last = self.leader_index
        res = dict(start=self.local_index + 1,
                   end=last)
        return res

    @property
    def need_rollback(self):
        if self.local_ahead:
            return True
        if self.leader_index < self.local_index:
            return True
        # unless something is really, weirdly wrong,
        # the leader will never have an uncommitted
        # record when a follower has the same
        # record but already committed.
        # so no test like this:
        # if self.leader_commit < self.local_commit:
        return False
    
    @property
    def rollback_to(self):
        if not self.need_rollback:
            return None
        if self.leader_index == -1:
            return -1
        return self.leader_index
    
    @property
    def local_ahead(self):
        if self.local_index > self.leader_index:
            return True
        # leader commit can be less than leader index
        if self.local_index > self.leader_commit:
            return True
        if self.local_commit > self.leader_commit:
            return True
        return False
    
        
class Follower(Voter):

    my_type = "follower"
    
    def __init__(self, server, timeout=0.75):
        super().__init__(server, self.my_type)
        self.timeout = timeout
        # get this too soon and logging during testing does not work
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        self.leader_addr = None
        self.heartbeat_count = 0
        self.substate = Substate.starting
        self.leaderless_timer = None
        self.last_vote = None
        self.last_vote_time = None
        self.debug_dumper = False
        
    def __str__(self):
        return "follower"
    
    def get_leader_addr(self):
        return self.leader_addr
    
    def start(self):
        if self.terminated:
            raise Exception("cannot start a terminated state")
        log = self.server.get_log()
        interval = self.election_interval()
        self.leaderless_timer = self.server.get_timer("follower-election",
                                                      log.get_term(),
                                                      interval,
                                                      self.leader_lost)
        self.leaderless_timer.start()

    async def stop(self):
        # ignore already terminated, just make sure it is done
        self.terminated = True
        if not self.leaderless_timer.terminated:
            await self.leaderless_timer.terminate()
        
    def election_interval(self):
        return random.uniform(self.timeout, 2 * self.timeout)

    async def leader_lost(self):
        if self.terminated:
            return
        await self.set_substate(Substate.leader_lost)
        return await self.start_election()
    
    async def start_election(self):
        if self.terminated:
            return
        try:
            sm = self.server.get_state_map()
            sm.start_state_change("follower", "candidate")
            self.terminated = True
            await self.leaderless_timer.terminate()
            self.logger.debug("starting election")
            self.logger.debug("doing switch to candidate")
            candidate = await sm.switch_to_candidate(self)
            await self.stop()
        except: # pragma: no cover error
            self.logger.error(traceback.format_exc())
            raise

    def decode_state(self, message):
        #
        # When we get a message from the leader, there are a number
        # of values in the message that we need to compare to the
        # local equivalents. All such values are in the log, and
        # an empty log on either side means that we might have None
        # values for things that are normally ints. Awkward. So
        # lets clean this up by converting nones to meaninginfull
        # ints and flags in the clearest way we can
        data = message.data
        log = self.server.get_log()
        last_rec = log.read()
        local_is_empty = False
        if last_rec is None:
            local_index = -1
            local_commit = -1
            local_prev_term = -1
            local_is_empty = True
        else:
            local_index = last_rec.index
            local_prev_term = last_rec.term
            local_is_empty = False
            local_commit = log.get_commit_index()
            if local_commit is None:
                local_commit = -1
        local_term = log.get_term()
        if local_term is None:
            local_term = -1
        leader_term = message.term
        leader_commit = data['leaderCommit']
        leader_index = data["prevLogIndex"]
        leader_prev_term = data["prevLogTerm"]
        if leader_commit is None:
            leader_commit = -1
        if leader_index is None:
            leader_index = -1
        if leader_prev_term is None:
            leader_prev_term = -1

        return StateDiff(local_is_empty,
                         local_index,
                         local_commit,
                         local_term,
                         local_prev_term,
                         leader_term,
                         leader_prev_term,
                         leader_commit,
                         leader_index,
                         data.get('leaderPort', None))

    async def on_heartbeat(self, message):
        # reset timeout
        self.heartbeat_logger.debug("resetting leaderless_timer on heartbeat")
        await self.leaderless_timer.reset()
        self.heartbeat_logger.debug("heartbeat from %s", message.sender)
        sd = self.decode_state(message) 
        data = message.data
        laddr = (sd.leader_addr[0], sd.leader_addr[1])
        if self.debug_dumper:
            self.logger.debug("starting on_heartbeat with \n%s\nand\n%s",
                              data, sd)
        self.heartbeat_count += 1
        if self.leader_addr != laddr:
            if self.leader_addr is None:
                await self.set_substate(Substate.joined)
            else:
                await self.set_substate(Substate.new_leader)
            self.leader_addr = laddr
            self.heartbeat_count = 0
        if sd.in_sync:
            if self.debug_dumper:
                self.logger.debug("state diff says we are in sync")
            await self.on_heartbeat_common(message)
        elif sd.needed_records is not None:
            if self.debug_dumper:
                self.logger.debug("state diff says we need records")
            await self.do_log_pull(message)
        elif sd.need_rollback:
            self.logger.info("heartbeat state diff says we need rollback")
            await self.do_rollback(message)
        else:
            msg = 'state diff decode failed to determine action'
            self.logger.error(msg + "\n%s\n%s", message.data, sd)
            raise Exception('state diff decode failed to determine action')
        self.heartbeat_logger.debug("heartbeat reply send")
        await self.set_substate(Substate.synced)
        return True

    async def do_log_pull(self, message):
        # just tell the leader where we are and have him
        # send what we are missing

        log = self.server.get_log()
        sd = self.decode_state(message) 
        self.logger.info("asking leader for log pull starting at %d",
                         sd.needed_records['start'])
        message = LogPullMessage(
            self.server.endpoint,
            message.sender,
            log.get_term(),
            {
                "start_index": sd.needed_records['start']
            }
        )
        if self.debug_dumper:
            self.logger.debug("prepped log pull message for leader\n%s",
                              message.data)
        await self.server.send_message_response(message)
        if self.debug_dumper:
            self.logger.debug("sent log pull message to leader\n%s",
                              message.data)
        # getting here might have been slow, let's reset the
        # timer
        if not self.terminated:
            await self.leaderless_timer.reset()

    async def do_rollback(self, message):
        # figure out what the leader's actual
        # state is and roll back to that. 
        log = self.server.get_log()
        data = message.data
        sd = self.decode_state(message)
        self.logger.debug("doing rollback %s", sd)
        if not sd.same_term:
            self.logger.info("rollback setting log term to %s, was %s",
                             message.term, log.get_term())
            log.set_term(message.term)
        last_rec = sd.rollback_to
        # if leader sends a reset, last_rec might be None
        if last_rec == -1 or last_rec is None:
            # our whole log is bogus?!!!
            self.logger.warning("Leader log is empty, ours is not, discarding"\
                                " everything")
            log.clear_all()
            return
        log.trim_after(last_rec)
        log.commit(last_rec)
        self.logger.debug("rolled back to message %d", last_rec)
    
    async def on_log_pull_response(self, message):
        self.logger.debug("in log pull response")
        await self.leaderless_timer.reset()
        data = message.data
        log = self.server.get_log()
        sd = self.decode_state(message) 
        if len(data["entries"]) == 0:
            self.logger.warning("log pull response containted no entries!")
            if data.get('code', None) == "reset":
                self.logger.warning("log pull response requires reset of log")
                await self.do_rollback(message)
            return
        self.logger.debug("updating log with %d entries",
                          len(data["entries"]))
        for ent in data["entries"]:
            log.append([LogRec(term=ent['term'],
                               user_data=ent['user_data']),])
            new_rec = log.read()
            commit = False
            if ent['committed']:
                log.commit(new_rec.index)
                commit = True
            if self.debug_dumper:
                self.logger.debug("added log rec %d, commit = %s",
                                  new_rec.index, commit)
        return True
        
    async def on_append_entries(self, message):
        self.logger.info("resetting leaderless_timer on append entries")
        await self.leaderless_timer.reset()
        log = self.server.get_log()
        data = message.data
        self.logger.debug("append %s %s", message, message.data)
        sd = self.decode_state(message)
        if not sd.same_term:
            self.logger.debug("on_append updating term from %s to %s",
                              sd.local_term, sd.leader_term)
            log.set_term(sd.leader_term)
        laddr = (sd.leader_addr[0], sd.leader_addr[1])
        if self.leader_addr != laddr:
            if self.leader_addr is None:
                await self.set_substate(Substate.joined)
            else:
                await self.set_substate(Substate.new_leader)
            self.leader_addr = laddr
        if sd.needed_records is not None:
            await self.send_response_message(message, votedYes=False)
            await self.do_log_pull(message)
            return True
        elif sd.need_rollback:
            await self.send_response_message(message, votedYes=False)
            self.logger.info("on_append_entries doing rollback to match leader")
            self.logger.debug("StateDiff is %s", sd)
            await self.do_rollback(message)
            return True
            
        # log the records in the message
        if len(data['entries']) == 0:
            start = sd.local_commit
            if start == -1:
                start = 0
            end = sd.leader_commit + 1
            self.logger.debug("on_append_entries commit %s to %s",
                              start, end-1)
            for i in range(start, end):
                log.commit(i)
            if False:
                await self.send_response_message(message)
                self.logger.info("Sent log update ack %s, local last rec = %d",
                                 message, log.read().index)
            return True
        entry_count = len(data["entries"]) 
        self.logger.debug("on_append_entries %d entries message %s",
                          entry_count, message)
        await self.set_substate(Substate.log_appending)
        self.logger.debug("updating log with %d entries",
                          len(data["entries"]))
        for ent in data["entries"]:
            log.append([LogRec(term=ent['term'], user_data=ent['user_data']),])
            readback = log.read()
            if ent['committed']:
                log.commit(readback.index)
        await self.send_response_message(message)
        self.logger.info("Sent log update ack %s, local last rec = %d",
                         message, log.read().index)
        return True
    
    async def send_response_message(self, msg, votedYes=True):
        log = self.server.get_log()
        data = {
            "response": votedYes,
            "currentTerm": log.get_term(),
        }
        data.update(msg.data)
        response = AppendResponseMessage(
            self.server.endpoint,
            msg.sender,
            msg.term,
            data
        )
        await self.server.send_message_response(response)
        logger = logging.getLogger(__name__)
        logger.info("sent response to %s term=%d %s",
                    response.receiver, response.term, data)

    async def on_term_start(self, message):
        self.logger.info("resetting leaderless_timer on term start")
        await self.leaderless_timer.reset()
        log = self.server.get_log()
        self.logger.info("follower got term start: message.term = %s"\
                         " local_term = %s",
                         message.term, log.get_term())
        log.set_term(message.term)
        data = message.data
        self.last_vote = None
        laddr = (data['leaderPort'][0], data['leaderPort'][1])
        if self.leader_addr != laddr:
            if self.leader_addr is None:
                self.logger.debug("follower setting new substate %s",
                                 Substate.joined)
                await self.set_substate(Substate.joined)
            else:
                self.logger.debug("follower setting new substate %s",
                                 Substate.new_leader)
                await self.set_substate(Substate.new_leader)
            self.leader_addr = laddr
        return True

    async def on_vote_request(self, message):
        # For some reason I can't figure out, this
        # message tends to come in during the process
        # of switching states, so it can end up here
        # despite the check for terminated in the base_state code.
        # Since everbody's awaitn stuff, I guess it can just happen
        if self.terminated:
            return False
        await self.leaderless_timer.reset() 
        # If this node has not voted,
        # and if lastLogIndex in message
        # is not earlier than our local log index
        # then we agree that the sender's claim
        # to be leader can stand, so we vote yes.
        # If we have not voted, but the sender's claim
        # is earlier than ours, then we vote no. If no
        # claim ever arrives with an up to date log
        # index, then we will eventually ask for votes
        # for ourselves, and will eventually win because
        # our last log record index is max.
        # If we have already voted, then we say no. Election
        # will resolve or restart.
        sd = self.decode_state(message) 
        log = self.server.get_log()
        # get the last record in the log
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        approve = False
        if self.last_vote is None and sd.local_is_empty:
            self.logger.info("everything None, voting true")
            approve = True
        elif self.last_vote is None and not sd.local_ahead:
            self.logger.info("last vote None, logs match, voting true")
            approve = True
        elif self.last_vote == message.sender:
            self.logger.info("last vote matches sender %s", message.sender)
            approve = True
        if approve:
            self.last_vote = message.sender
            self.last_vote_time = time.time()
            self.logger.info("voting true")
            await self.send_vote_response_message(message, votedYes=True)
            self.logger.info("resetting leaderless_timer on vote done")
            # election in progress, let it run
        else:
            self.logger.info("voting false on message %s %s",
                             message, message.data)
            self.logger.info("my last vote = %s", self.last_vote)
            await self.send_vote_response_message(message, votedYes=False)
            self.last_vote_time = time.time()
        return True
        
    async def on_client_command(self, message):
        await self.dispose_client_command(message, self.server)
        return True

    async def on_append_response(self, message): # pragma: no cover error
        self.logger.warning("follower unexpectedly got append response from %s",
                            message.sender)
        return True
    
    async def on_vote_received(self, message): # pragma: no cover error
        log = self.server.get_log()
        self.logger.info("follower unexpectedly got vote:"\
                         " message.term = %s local_term = %s",
                         message.term, log.get_term())
        return True

    async def on_heartbeat_response(self, message):  # pragma: no cover error
        self.logger.warning("follower unexpectedly got heartbeat"
                            " response from %s",  message.sender)
        return True
