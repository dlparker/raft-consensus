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
    def leader_index_none(self):
        return self.leader_index == -1
    
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
    def rollback_to(self):
        if not self.local_ahead:
            return None
        if self.leader_index == -1:
            return None
        if self.leader_commit == -1:
            return None
        if self.leader_commit < self.leader_index:
            return self.leader_commit
        return self.leader_index
    
    @property
    def local_ahead(self):
        if self.local_index > self.leader_index:
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
                         leader_index)

        
    
    async def on_heartbeat(self, message):
        # reset timeout
        self.heartbeat_logger.debug("resetting leaderless_timer on heartbeat")
        await self.leaderless_timer.reset()
        self.heartbeat_logger.debug("heartbeat from %s", message.sender)
        data = message.data
        laddr = (data["leaderPort"][0], data["leaderPort"][1])
        self.heartbeat_count += 1
        if self.leader_addr != laddr:
            if self.leader_addr is None:
                await self.set_substate(Substate.joined)
            else:
                await self.set_substate(Substate.new_leader)
            self.leader_addr = laddr
            self.heartbeat_count = 0
        if await self.do_sync_action(message):
            await self.on_heartbeat_common(message)
            self.heartbeat_logger.debug("heartbeat reply send")
            await self.set_substate(Substate.synced)
        return True

    async def on_log_pull_response(self, message):
        self.logger.debug("in log pull response")
        await self.leaderless_timer.reset()
        data = message.data
        log = self.server.get_log()
        last_rec = log.read()
        if len(data["entries"]) == 0:
            self.logger.warning("log pull response containted no entries!")
            if data.get('code', None) == "reset":
                self.logger.warning("log pull response requires reset of log")
                last_index = data['prevLogIndex']
                if last_index is None:
                    self.logger.warning("Leader says log is empty!")
                    log.clear_all()
                elif last_index < last_rec.index:
                    self.logger.warning("Trimming log back to %s", last_index)
                    log.trim_after(last_index)
            return
        self.logger.debug("updating log with %d entries",
                          len(data["entries"]))
        for ent in data["entries"]:
            log.append([LogRec(term=ent['term'], user_data=ent['user_data']),])
            if ent['committed']:
                log.commit(ent['index'])
        leader_commit = data['leaderCommit']
        return True
        
    async def do_sync_action(self, message):
        data = message.data
        leader_commit = data['leaderCommit']
        leader_last_rec_index = data["prevLogIndex"]
        leader_last_rec_term = data["prevLogTerm"]
        log = self.server.get_log()
        last_rec = log.read()
        local_commit = log.get_commit_index()

        # before comparing log index and commit index, we need
        # to make sure we are operating in the same term as the
        # leader
        local_term = log.get_term()
        if local_term != message.term:
            if local_term is None:
                self.logger.info("changing local term to match leader %d",
                                 message.term)
                log.set_term(message.term)
            elif message.term is None:
                self.logger.warning("Leader says term is %d but we think %d," \
                                    " doing rollback",
                                    message.term, local_term)
                await self.do_rollback_to_leader(message)
                return False
            elif message.term > local_term:
                self.logger.info("changing local term to match leader %d",
                                 message.term)
                log.set_term(message.term)
            else:
                self.logger.warning("Leader says term is %d but we think %d," \
                                    " doing rollback",
                                    message.term, local_term)
                await self.do_rollback_to_leader(message)
                return False
        
        if not last_rec:
            # local log is empty
            if leader_last_rec_index is None:
                # all logs are empty (ish), that's in sync,
                # just return a hearbeat response
                return True
            # we got nothing, leader got something, get it all
            self.logger.debug("leader sent index %d, but our" \
                              " log is empty, asking for pull",
                              leader_commit)
            await self.do_log_pull(message)
            return False
        if leader_last_rec_index is None:
            # we have something, leader has nothing,
            # we have a problem, so rollback
            await self.do_rollback_to_leader(message)
            return False
        # can trust last_rec.index and leader_last_rec_index for comparisons
        # at this point after null tests above
        if (last_rec.index == leader_last_rec_index
            and leader_commit == local_commit):
            # all same, just heartbeat
            return True
        # no matter what the commit indexes say, the
        # record indexes can tell us we need pull or rollback
        if last_rec.index < leader_last_rec_index:
            self.logger.info("Leader says last index %d our last index is" \
                             " %d, asking for pull",
                             leader_last_rec_index, last_rec.index)
            await self.do_log_pull(message)
            return False
        if last_rec.index > leader_last_rec_index:
            self.logger.info("Leader says last index %d our last index is" \
                             " %d, doing rollback",
                             leader_last_rec_index, last_rec.index)
            await self.do_rollback_to_leader(message)
            return False
        #
        # We didn't get a direction from the indexes, see if
        # the commit indexes say something clear
        # this could be None == None or int == int
        if leader_commit == local_commit:
            # nothing needs recording
            return True
        if leader_commit is None:
            # local_commit cannot be None or above would match
            self.logger.warning("Leader says commit is %d but " \
                                " we think %d," \
                                " doing rollback",
                                leader_commit, local_commit)
            await self.do_rollback_to_leader(message)
            return False
        if leader_commit < local_commit:
            self.logger.warning("Leader says commit is %d but " \
                                " we think %d," \
                                " doing rollback",
                                leader_commit, local_commit)
            await self.do_rollback_to_leader(message)
            return False
        if leader_commit <= last_rec.index:
            # we have the committed record, so commit from
            # wherever we are up to the leader_commit index
            self.logger.info("updating local commit index to match " \
                             " leader %d",
                             leader_commit)
            await self.set_substate(Substate.syncing_commit)
            for i in range(local_commit, leader_commit + 1):
                log.commit(i)
                return True
        # At this point there are no more out of sync conditions to
        # detect, so just reply with a hearbeat
        return True
        
    async def do_log_pull(self, message):
        # just tell the leader where we are and have him
        # send what we are missing

        log = self.server.get_log()
        last_rec = log.read()
        if last_rec is None:
            start_index = 0
        else:
            start_index = last_rec.index + 1
        self.logger.info("asking leader for log pull starting at %d",
                         start_index)
        message = LogPullMessage(
            self.server.endpoint,
            message.sender,
            log.get_term(),
            {
                "start_index": start_index
            }
        )
        await self.server.send_message_response(message)
        # getting here might have been slow, let's reset the
        # timer
        if not self.terminated:
            await self.leaderless_timer.reset()

    async def do_rollback_to_leader(self, message):
        # figure out what the leader's actual
        # state is and roll back to that. 
        
        data = message.data
        leader_commit = data['leaderCommit']
        leader_last_rec_index = data["prevLogIndex"]
        leader_last_rec_term = data["prevLogIndex"]
        log = self.server.get_log()
        last_rec = log.read()
        self.logger.info("setting log term to %d, was %d", message.term, log.get_term())
        log.set_term(message.term)
        if last_rec is None:
            self.logger.warning("in call to rollback, we have nothing in the log, nothing to do")
            return
        if leader_commit is None:
            # our whole log is bogus?!!!
            self.logger.warning("Leader log is empty, ours is not, discarding"\
                                " everything")
            log.clear_all()
            return
        if last_rec.index <= leader_commit:
            self.logger.warning("in call to rollback, our log matches leader commit, nothing to do")
            return
        self.logger.warning("in call to rollback, discarding messages after %d, to %d",
                            leader_commit, last_rec.index)
        log.trim_after(leader_commit)
        log.commit(leader_commit)
            
    async def on_append_entries(self, message):
        self.logger.info("resetting leaderless_timer on append entries")
        await self.leaderless_timer.reset()
        log = self.server.get_log()
        data = message.data
        self.logger.debug("append %s %s", message, message.data)
        laddr = (data["leaderPort"][0], data["leaderPort"][1])
        if self.leader_addr != laddr:
            if self.leader_addr is None:
                await self.set_substate(Substate.joined)
            else:
                await self.set_substate(Substate.new_leader)
            self.leader_addr = laddr
        entry_count = len(data["entries"]) 

        self.logger.debug("on_append_entries %d entries message %s",
                          entry_count, message)
        if log.get_term() and message.term < log.get_term():
            await self.send_response_message(message, votedYes=False)
            self.logger.info("rejecting message because sender term is less than mine %s", message)
            return True
        # log the records in the message
        if entry_count > 0:
            await self.set_substate(Substate.log_appending)
            self.logger.debug("updating log with %d entries",
                              len(data["entries"]))
            for ent in data["entries"]:
                log.append([LogRec(term=ent['term'], user_data=ent['user_data']),])
                if ent['committed']:
                    log.commit(ent['index'])
            await self.send_response_message(message)
            self.logger.info("Sent log update ack %s, local last rec = %d",
                             message, log.read().index)
            return True
        # must be a commit trigger for an existing record
        leader_commit = data['leaderCommit']
        leader_last_rec_index = data["prevLogIndex"]
        leader_last_rec_term = data["prevLogIndex"]
        last_rec = log.read()
        if last_rec is None:
            self.logger.info("leader sent an append message with no entries, but we have no local log records, trying to sync up")
            if await self.do_sync_action(message):
                await self.send_response_message(message)
                self.logger.info("Sent log update ack %s", message)
                return True
        local_commit = log.get_commit_index()
        self.logger.debug("append leader_commit %s, local_commit %s," \
                          " leader_last_index %s, last_rec.index %s",
                          leader_commit, local_commit,
                          leader_last_rec_index, last_rec.index)
        if leader_commit is not None:
            if last_rec.index >= leader_commit:
                if local_commit is not None:
                    start = local_commit
                else:
                    start = 0
                if start <= leader_commit:
                    self.logger.debug("Leader sent commit %d, local is %s committing from %d",
                                      leader_commit, local_commit, start)
                    for i in range(start, leader_commit + 1):
                        log.commit(i)
                    return True
        # we are not in sync, fix that
        self.logger.debug("append entry empty and indexes and commits did not make sense against local log, trying to sync with leader")
        if await self.do_sync_action(message):
            await self.send_response_message(message)
            self.logger.info("Sent log update ack %s", message)

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
        laddr = (data["leaderPort"][0], data["leaderPort"][1])
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
        if self.last_vote is None and last_index is None:
            self.logger.info("everything None, voting true")
            approve = True
        elif (self.last_vote is None 
              and message.data["lastLogIndex"] is None and last_rec is None):
            self.logger.info("last vote None, logs empty, voting true")
            approve = True
        elif (self.last_vote is None
              and message.data["lastLogIndex"] is not None
              and message.data["lastLogIndex"] >= last_index):
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
