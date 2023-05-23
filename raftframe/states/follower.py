import random
import time
import asyncio
from dataclasses import dataclass, asdict
import logging
import traceback

from raftframe.utils import task_logger
from raftframe.log.log_api import LogRec
from raftframe.messages.append_entries import AppendResponseMessage
from raftframe.messages.request_vote import RequestVoteResponseMessage
from raftframe.messages.heartbeat import HeartbeatResponseMessage
from .base_state import State, Substate, StateCode

class Follower(State):

    my_code = StateCode.follower
    
    def __init__(self, server, timeout=0.75):
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        super().__init__(server, self.my_code)
        # get this too soon and logging during testing does not work
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        self.leader_addr = None
        self.heartbeat_count = 0
        self.substate = Substate.starting
        self.leaderless_timer = None
        self.last_vote = None
        self.last_vote_term = None
        
    def __str__(self):
        return "follower"
    
    def get_leader_addr(self):
        return self.leader_addr
    
    def start(self):
        if self.terminated:
            raise Exception("cannot start a terminated state")
        self.logger = logging.getLogger(__name__)
        interval = self.election_interval()
        self.leaderless_timer = self.server.get_timer("follower-election",
                                                      self.log.get_term(),
                                                      interval,
                                                      self.leader_lost)
        self.leaderless_timer.start()
        self.logger.debug("start complete")

    async def stop(self):
        # ignore already terminated, just make sure it is done
        self.terminated = True
        if self.leaderless_timer and not self.leaderless_timer.terminated:
            await self.leaderless_timer.terminate()
            self.leaderless_timer = None
        
    def election_interval(self):
        return random.uniform(self.timeout, 2 * self.timeout)

    async def leader_lost(self):
        # This is here only to ensure that a rare (impossible?)
        # race condition is not a problem. That condition might
        # happen if the timer fires but this method has is pending
        # and before it can run the stop sequence or the start election
        # sequence runs, sets terminated, then runs terminate on the
        # timer. It seems possible that this method could run before
        # the terminate method runs, so we check for that condition
        # and return. Writing a test for this would require so much
        # fiddling with the code that I am unconvinced that it would
        # improve it, and suspect it might degrade it. So I pragma
        # around it.
        if self.terminated: # pragma: no cover race
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
            sm = self.server.get_state_map()
            sm.failed_state_change("follower","candidate",
                                   traceback.format_exc())
            raise

    async def on_heartbeat(self, message):
        # Heartbeat is like an AppendEntries RPC which
        # the leader sends when it has no new log entries to share.
        # In the doc, the protocol just uses and AppendEntries RPC
        # with no new entries. This variation in the protocol
        # is to help the programmer reason better about the behavior
        # of the system by making this distinction.
        if message.term <  self.log.get_term():
            self.logger.info("heartbeat -> false leader %s term less than " \
                                        "local %s, rejecting",
                             message.term, self.log.get_term())
            data = dict(success=False,
                        prevLogIndex=message.prevLogIndex,
                        prevLogTerm=message.prevLogTerm,
                        last_index=self.log.get_last_index(),
                        last_term=self.log.get_last_term())
            reply = HeartbeatResponseMessage(message.receiver,
                                             message.sender,
                                             term=self.log.get_term(),
                                             data=data)
            await self.server.post_message(reply)
            return True
        self.heartbeat_logger.debug("resetting leaderless_timer on heartbeat")
        await self.leaderless_timer.reset()
        self.heartbeat_logger.debug("heartbeat from %s", message.sender)
        self.heartbeat_count += 1
        laddr = message.sender
        if self.leader_addr is None:
            self.leader_addr = laddr
            self.heartbeat_count = 0
            self.last_vote = None
            self.last_vote_term = None
            self.logger.debug("setting substate to joined")
            await self.set_substate(Substate.joined)
        if message.term > self.log.get_term():
            self.logger.info("heartbeat -> leader %s term different " \
                                        "from local %s, updating",
                              message.term, self.log.get_term())
            self.log.set_term(message.term)

        # We are in sync if the our last log record and the
        # leader's last log record are the same, which
        # means both the index and the term must match
        if (self.log.get_last_index() == message.prevLogIndex
            and self.log.get_last_term() == message.prevLogTerm):
            self.heartbeat_logger.debug("heartbeat up to date, sending")
            success = True
        else:
            success = False
            msg = "heartbeat difference found, expecting AppendEntry" \
                  " messages to resolve it, " \
                  " leader index=%d, term=%d, local index=%d, term = %d"
            self.logger.debug(msg, message.prevLogIndex,
                              message.prevLogTerm,
                              self.log.get_last_index(),
                              self.log.get_last_term())
            self.heartbeat_logger.debug(msg, message.prevLogIndex,
                                        message.prevLogTerm,
                                        self.log.get_last_index(),
                                        self.log.get_last_term())
                                        
        data = dict(success=success,
                    prevLogIndex=message.prevLogIndex,
                    prevLogTerm=message.prevLogTerm,
                    last_index=self.log.get_last_index(),
                    last_term=self.log.get_last_term())
        reply = HeartbeatResponseMessage(message.receiver,
                                         message.sender,
                                         term=self.log.get_term(),
                                         data=data)
        await self.server.post_message(reply)
        if success:
            await self.set_substate(Substate.synced)
        else:
            await self.set_substate(Substate.out_of_sync)
        await self.leaderless_timer.reset()
        return True

    async def on_append_entries(self, message):
        # first make sure it is not some rogue claimant to the throne
        if message.term < self.log.get_term():
            # This means leader should resign, we have
            # been talking to a later leader
            self.logger.info("leader %s term %s is less than local %s, " \
                                        "telling leader about it",
                             message.sender, message.term, self.log.get_term())
            msg = f"leader term is {message.term} but ours is " \
                f"{self.log.get_term()}"
            self.server.record_illegal_message_state(message.sender,
                                                     msg, message.data)
            return await self.do_bad_append(message)
        self.logger.info("resetting leaderless_timer on append entries")
        await self.leaderless_timer.reset()
        self.logger.debug("append prev_term = %d prev_log = %d commit = %d %s %s",
                          message.prevLogTerm, message.prevLogIndex, message.leaderCommit,
                          message, message.data)
        laddr = message.sender
        if self.leader_addr is None or self.leader_addr != laddr:
            self.leader_addr = laddr
            self.last_vote = None
            self.last_vote_term = None
            self.heartbeat_count = 0
            self.logger.debug("setting substate to joined new leader %s", laddr)
            await self.set_substate(Substate.joined)

        # Regardless of the log ops, make sure we agree with leader as
        # to what term it is
        if message.term > self.log.get_term():
            self.logger.info("on_append -> leader %s term different, " \
                                        "updating local %s",
                              message.term, self.log.get_term())
            self.log.set_term(message.term)
        # If we don't have the record prior to the current appends,
        # the leader should backdown till we do
        if self.log.get_last_index() < message.prevLogIndex:
            self.logger.info("leader %s last index %d ours %d, " \
                                        "telling leader about it",
                             message.sender, message.prevLogIndex,
                             self.log.get_last_index())
            return await self.do_bad_append(message)

        # Current term matches, we have the previous record.
        # Now check to see if our copy has the same term
        # as the leader's copy.
        # let's check for the "everything is right" condition,
        # which should be the common case
        if message.prevLogIndex == 0:
            self.logger.info("leader and our log empty, its a match")
            return await self.do_good_append(message)
        prev_rec = self.log.read(message.prevLogIndex)
        if prev_rec.term != message.prevLogTerm:
            self.logger.info("leader %s last record term %d" \
                             " ours %d, telling leader about it",
                             message.sender, message.prevLogTerm,
                             prev_rec.term)
            return await self.do_bad_append(message)
        self.logger.info("leader and our log agree at index %d",
                         prev_rec.index)
        if self.log.get_last_index() == message.prevLogIndex:
            # This is the most common case, appending along
            # without anybody getting out of sync
            return await self.do_good_append(message)
            
        # Leader must be resending or overwriting.
        # Overwriting can happen due to leadership changes.
        # Resending can happen due to race between AppendEntries
        # and Heartbeat processing, due to lack of order in
        # async processing. We should delete everything
        # in the log after the marker record if it matches
        replace_rec = self.log.read(message.prevLogIndex + 1)
        new_rec = message.data['entries'][0]
        if replace_rec.term == new_rec['term']:
            self.logger.info("re-write of record %d", replace_rec.index)
        else:
            self.logger.info("overwrite of record %d", replace_rec.index)
        return await self.do_good_append(message)

    async def do_bad_append(self, message):
        data = dict(success=False,
                    last_index=self.log.get_last_index(),
                    last_term=self.log.get_last_term())
        reply = AppendResponseMessage(message.receiver,
                                      message.sender,
                                      term=self.log.get_term(),
                                      data=data,
                                      prevLogTerm=message.prevLogTerm,
                                      prevLogIndex=message.prevLogIndex,
                                      leaderCommit=message.leaderCommit)
        
        await self.server.post_message(reply)
        await self.set_substate(Substate.out_of_sync)
        return True

    async def do_commit_append(self, message):
        # per protocol, implies commit all previous records
        # log record indices start at 1, so
        # we never commit 0
        start = max(self.log.get_commit_index(), 1)
        end = message.leaderCommit + 1
        self.logger.debug("on_append_entries commit %s through %s",
                          start, end-1)
        for i in range(start, end):
            self.log.commit(i)
                
        data = dict(success=True,
                    last_index=self.log.get_last_index(),
                    last_term=self.log.get_last_term(),
                    commit_only=True)
        
        reply = AppendResponseMessage(message.receiver,
                                      message.sender,
                                      term=self.log.get_term(),
                                      data=data,
                                      prevLogIndex=message.prevLogIndex,
                                      prevLogTerm=message.prevLogTerm,
                                      leaderCommit=message.leaderCommit)

        await self.server.post_message(reply)
        self.logger.debug("Sent log commit ack\nresponse %s", data)
        return True
    
    async def do_good_append(self, message):

        if message.data.get('commitOnly', False):
            return await self.do_commit_append(message)
            
        entry_count = len(message.data["entries"]) 
        self.logger.debug("on_append_entries %d entries message %s",
                          entry_count, message)
        await self.set_substate(Substate.out_of_sync)
        last_commit = 0
        last_entry_index = 0
        for ent in message.data["entries"]:
            rec = LogRec.from_dict(ent)
            # it could be a new record, or it could
            # be a replacement of an old record.
            new_rec = self.log.replace_or_append(rec)
            last_entry_index = new_rec.index
            if new_rec.committed:
                last_commit = new_rec.index
        if self.log.get_commit_index() < last_commit:
            self.log.commit(last_commit)
        data = dict(success=True,
                    last_entry_index=last_entry_index,
                    last_index=self.log.get_last_index(),
                    last_term=self.log.get_last_term())
        reply = AppendResponseMessage(message.receiver,
                                      message.sender,
                                      term=self.log.get_term(),
                                      data=data,
                                      prevLogIndex=message.prevLogIndex,
                                      prevLogTerm=message.prevLogTerm,
                                      leaderCommit=message.leaderCommit)
                                      
        await self.server.post_message(reply)
        self.logger.debug("Sent log update ack, local last rec = %d," \
                          " commit = %d \nresponse %s",
                          last_entry_index, last_commit, data)
        return True
    
    async def on_vote_request(self, message):
        # Used to have this code, but could not make it happen
        # during testing, so I think it is not actually possible
        # anymore. Some refactoring and cleanup probably eliminated it.
        # if self.terminated:
        #    self.logger.error("follower got on_vote_request message after terminated")
        #    return False
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
        last_index = self.log.get_last_index()
        last_term = self.log.get_last_term()
        approve = False
        if message.term < self.log.get_term():
            self.logger.info("voting false, term %s should at least " \
                             " local term %s",
                             message.term, self.log.get_term())
        elif (last_index > message.prevLogIndex
              or last_term > message.prevLogTerm):
            self.logger.info("voting false, last local log record "
                             " newer than candidate's")
        elif self.last_vote is None:
            self.logger.info("Not voted yet, voting true")
            approve = True
        elif self.last_vote_term < message.term:
            self.logger.info("Old vote for old term, voting true")
            approve = True
        # used to have the following code, but I don't think it can
        # happen, if you receive a vote request from the same sender
        # twice, then the sender must have increased the term according
        # to protocol
        # elif self.last_vote == message.sender:
        #     self.logger.info("last vote matches sender %s", message.sender)
        #     approve = True
        if approve:
            self.last_vote = message.sender
            self.last_vote_term = message.term
            self.logger.info("voting true")
            await self.send_vote_response_message(message, votedYes=True)
            self.logger.info("resetting leaderless_timer on vote done")
            # election in progress, let it run
        else:
            self.logger.info("voting false on message %s %s",
                             message, message.data)
            self.logger.info("my last vote = %s, index %d, last term %d",
                             self.last_vote, last_index, last_term)
            await self.send_vote_response_message(message, votedYes=False)
        if message.term > self.log.get_term():
            # regardless, increase the term to no less than that of the
            # sender
            self.log.set_term(message.term)
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
                         message.term, self.log.get_term())
        return True

    async def on_heartbeat_response(self, message):  # pragma: no cover error
        self.logger.warning("follower unexpectedly got heartbeat"
                            " response from %s",  message.sender)
        return True

    async def send_vote_response_message(self, message, votedYes=True):
        vote_response = RequestVoteResponseMessage(
            self.server.endpoint,
            message.sender,
            message.term,
            {"response": votedYes})
        await self.server.send_message_response(vote_response)
    
