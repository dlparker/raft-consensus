import random
import time
import asyncio
from dataclasses import asdict
import logging
import traceback

from ..utils import task_logger
from ..log.log_api import LogRec
from ..messages.append_entries import AppendResponseMessage
from ..messages.log_pull import LogPullMessage
from .base_state import Substate
from .voter import Voter

class Follower(Voter):

    my_type = "follower"
    
    def __init__(self, server, timeout=0.75):
        super().__init__(server, self.my_type)
        self.timeout = timeout
        # get this too soon and logging during testing does not work
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        self.leaderless_timer = None
        self.switched = False
        self.leader_addr = None
        self.heartbeat_count = 0
        self.substate = Substate.starting
        interval = self.election_interval()
        log = server.get_log()
        self.leaderless_timer = self.server.get_timer("follower-election",
                                                      log.get_term(),
                                                      interval,
                                                      self.leader_lost)
        self.last_vote = None
        self.last_vote_time = None
        
    def __str__(self):
        return "follower"
    
    def get_leader_addr(self):
        return self.leader_addr
    
    def start(self):
        if self.terminated:
            raise Exception("cannot start a terminated state")
        self.leaderless_timer.start()

    async def stop(self):
        if self.terminated:
            return
        self.terminated = True
        await self.leaderless_timer.terminate()
        
    def election_interval(self):
        return random.uniform(self.timeout, 2 * self.timeout)

    async def leader_lost(self):
        await self.set_substate(Substate.leader_lost)
        if self.terminated:
            return
        return await self.start_election()
    
    async def start_election(self):
        if not self.leaderless_timer.is_enabled() or self.terminated:
            return
        if (self.last_vote_time and time.time() - self.last_vote_time <
            self.timeout):
            self.logger.error("timing problem")
            await asyncio.sleep(time.time() - self.last_vote_time)
            if self.switched or self.leader_addr is not None:
                return
        try:
            self.terminated = True
            self.leaderless_timer.disable()
            self.logger.debug("starting election")
            self.logger.debug("doing switch to candidate")
            sm = self.server.get_state_map()
            self.switched = True
            candidate = await sm.switch_to_candidate(self)
            await self.leaderless_timer.terminate()
        except:
            self.logger.error(traceback.format_exc())
            raise

    async def on_heartbeat(self, message):
        # reset timeout
        self.logger.info("resetting leaderless_timer on heartbeat")
        if self.leaderless_timer.is_enabled():
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

    async def on_log_pull_response(self, message):
        if self.leaderless_timer.is_enabled():
            await self.leaderless_timer.reset()
        data = message.data
        log = self.server.get_log()
        if len(data["entries"]) == 0:
            return
        self.logger.debug("updating log with %d entries",
                          len(data["entries"]))
        for ent in data["entries"]:
            log.append([LogRec(term=ent['term'], user_data=ent['user_data']),])
            if ent['committed']:
                log.commit(ent['index'])
        leader_commit = data['leaderCommit']
        return
        
    async def do_sync_action(self, message):
        data = message.data
        leader_commit = data['leaderCommit']
        leader_last_rec_index = data["prevLogIndex"]
        leader_last_rec_term = data["prevLogTerm"]
        log = self.server.get_log()
        last_rec = log.read()
        local_commit = log.get_commit_index()

        # The simplest case is that the local log and the leader log match. Look
        # for that first
        if not last_rec:
            # local log is empty
            if leader_commit is None:
                # all logs are empty (ish), that's in sync, just return a hearbeat response
                return True
            # we got nothing, leader got something, get it all
            self.logger.debug("leader sent commit for %d, but our log is empty, asking for pull",
                              leader_commit)
            await self.do_log_pull(message)
            return False
        elif leader_commit is not None:
            if last_rec.index >= leader_commit:
                if local_commit is None or local_commit < leader_commit:
                    # leader committed last record, and we didn't
                    # get the memo, so just apply it
                    await self.set_substate(Substate.syncing_commit)
                    if local_commit is None:
                        start = 0
                    else:
                        start = local_commit
                    self.logger.debug("leader sent commit for %d, local is %s committing from %d",
                                      leader_commit, local_commit, start)
                    for i in range(start, leader_commit + 1):
                        log.commit(i)
                    return True
        last_index = last_rec.index
        last_term = last_rec.term
        # Next simplest case is that our term is out of sync with the
        # leader. If so, we need to backout our local log records to match
        local_term = log.get_term()
        if local_term is not None and message.term < local_term:
            self.logger.warning("Leader says term is %d but we think %d, doing rollback",
                                message.term, local_term)
            await self.do_rollback_to_leader(message)
            return False

        # Next simplest case is that we have the same term as leader, there is stuff
        # in both logs, and our last record matches the leader's commit index. The
        # leader might have an additional record that is not committed yet, so we
        # do not key off of leader's index
        if leader_commit is not None and last_index < leader_commit:
            self.logger.info("Leader says commit is %d our last index is %d, asking for pull",
                                leader_commit, last_index)
            await self.do_log_pull(message)
            return False

        # At this point we know that the term at leader matches ours,
        # that the last committed record at the leader is present in our
        # local log. Now check and see that we have the same commit index
        # locally. If our commit is behind, just update it. If it is
        # ahead,  some complex failover and restart scenario
        # got us out of sync and ahead of the leader, so
        # do a rollback.
        if local_commit is not None and leader_commit is not None:
            if local_commit > leader_commit:
                self.logger.warning("Leader says commit is %d but ours is  %d, doing rollback",
                                    leader_commit, log.get_commit_index())
                self.logger.warning("message = %s %s", message, message.data)
                await self.do_rollback_to_leader(message)
                return False
            if local_commit < leader_commit:
                self.logger.info("Leader says commit is %d ours is %d, updating",
                                 leader_commit, log.get_commit_index())
                for i in range(local_commit, leader_commit + 1):
                    log.commit(i)

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
        self.logger.info("asking leader for log pull starting at %d", start_index)
        message = LogPullMessage(
            self.server.endpoint,
            message.sender,
            log.get_term(),
            {
                "start_index": start_index
            }
        )
        await self.server.send_message_response(message)

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
        if last_rec.index <= leader_commit:
            self.logger.warning("in call to rollback, our log matches leader commit, nothing to do")
            return
        self.logger.warning("in call to rollback, discarding messages after %d, to %d",
                            leader_commit, last_rec.index)
        log.trim_after(leader_commit)
        log.commit(leader_commit)
            
    async def on_append_entries(self, message):
        self.logger.info("resetting leaderless_timer on append entries")
        if self.leaderless_timer.is_enabled():
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
            return
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
            return
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
                return
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
                    return
        # we are not in sync, fix that
        self.logger.debug("append entry empty and indexes and commits did not make sense against local log, trying to sync with leader")
        if await self.do_sync_action(message):
            await self.send_response_message(message)
            self.logger.info("Sent log update ack %s", message)
    
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
        if self.leaderless_timer.is_enabled():
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

    async def on_vote_request(self, message): 
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
            if self.leaderless_timer.is_enabled():
                await self.leaderless_timer.reset() 
        else:
            self.logger.info("voting false on message %s %s",
                             message, message.data)
            self.logger.info("my last vote = %s", self.last_vote)
            await self.send_vote_response_message(message, votedYes=False)
            self.last_vote_time = time.time()
        return self, None
        
    async def on_client_command(self, message):
        await self.dispose_client_command(message, self.server)

    async def on_append_response(self, message): # pragma: no cover error
        self.logger.warning("follower unexpectedly got append response from %s",
                            message.sender)
    
    async def on_vote_received(self, message): # pragma: no cover error
        log = self.server.get_log()
        self.logger.info("follower unexpectedly got vote:"\
                         " message.term = %s local_term = %s",
                         message.term, log.get_term())

    async def on_heartbeat_response(self, message):  # pragma: no cover error
        self.logger.warning("follower unexpectedly got heartbeat"
                            " response from %s",  message.sender)
