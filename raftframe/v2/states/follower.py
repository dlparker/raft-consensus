import time
import traceback
import logging
import json
from raftframe.v2.log.log_api import LogRec
from raftframe.v2.states.base_state import StateCode, Substate, BaseState
from raftframe.v2.messages.append_entries import AppendResponseMessage
from raftframe.v2.messages.request_vote import RequestVoteResponseMessage

class Follower(BaseState):

    def __init__(self, hull):
        super().__init__(hull, StateCode.follower)
        # log is set in BaseState
        self.last_index = self.log.get_last_index()
        # only known after first accepted append_entries call
        self.leader_uri = None
        # only used during voting for leadership
        self.last_vote = None
        # Needs to be as recent as configured maximum silence period, or we raise hell.
        # Pretend we just got a call, that gives possible actual leader time to ping us
        self.last_leader_contact = time.time()

    async def start(self):
        await super().start()
        self.last_leader_contact = time.time()
        await self.run_after(self.hull.get_leader_lost_timeout(), self.contact_checker)
        
    async def on_append_entries(self, message):
        self.logger.debug("%s append term = %d prev_index = %d local_term = %d local_index = %d",
                          self.hull.get_my_uri(),  message.term,
                          message.prevLogIndex, self.log.get_term(), self.last_index)

        # Read the following three if statements carefully before modifying.
        # There are logic claims implicit in the fall through
        # Common case first, leader's idea of cluster state matches ours, no new records
        # for the log, a heartbeat, in other words
        if (message.term == self.log.get_term()
            and message.prevLogIndex == self.last_index and message.entries == []):
            if self.leader_uri is None:
                self.leader_uri = message.sender
                self.logger.info("%s accepting new leader %s", self.hull.get_my_uri(),
                                 self.leader_uri)
            else:
                self.logger.debug("%s heartbeat from leader %s", self.hull.get_my_uri(),
                                  message.sender)
            await self.send_append_entries_response(message, None)
            return
        # Very rare case, sender thinks it is leader but has old term, probably
        # a network partition, or some kind of latency problem with the claimant's
        # operations that made us have an election. Tell the sender it is not leader any more.
        if message.term < self.log.get_term():
            await self.send_reject_append_response(message)
            return
        self.last_leader_contact = time.time()
        # Rare case, election in progress and leader declaring itself winner, other
        # append entries logic the same as when local and leader have same term
        if message.term > self.log.get_term():
            self.logger.info("accepting leader %s after voting for %s", message.sender,
                             self.last_vote)
            self.leader_uri = message.sender
            self.last_vote = None
            if message.prevLogIndex == self.last_index:
                # no new records
                self.logger.debug("no new records, just election result")
                await self.send_append_entries_response(message, None)
                return
        # We know messag.term == term from first if test seive.
        # We know message.prevLogIndex is > self.last_index
        # because protocol guarantees it is not less (if code is correct)
        # because then we would be the leader, or some other server would
        # be, and the term would be wrong and we would reject this message above.
        # We know index is not equal cause of two checks above in first and
        # third if statement clauses
        self.logger.debug("new records")
        processor = self.hull.get_processor()
        recs = []
        for command in message.entries:
            result = None
            error = None
            try:
                result, error = await processor.process_command(command)
                if error is None:
                    self.logger.debug("processor ran no error")
                else:
                    self.logger.warning("processor ran but had an error")
            except Exception as e:
                trace = traceback.format_exc()
                msg = f"processor caused exception {trace}"
                self.logger.error(trace)
            recs.append(dict(result=result, error=error))
            run_result = dict(command=command,
                              result=result,
                              error=error)
            new_rec = LogRec(term=self.log.get_term(),
                             user_data=json.dumps(run_result))
            self.log.append([new_rec,])
        await self.send_append_entries_response(message, recs)
        return

    async def on_vote_request(self, message):
        if self.last_vote is not None:
            # we only vote once
            await self.send_vote_response_message(message, votedYes=False)
            return
        # Leadership claims have to be for max log commit index of
        # at least the same as our local copy
        last_index = self.log.get_last_index()
        # If the messages claim for log index or term are not at least as high
        # as our local values, then vote no.
        if message.prevLogIndex < last_index or message.term < self.log.get_term():
            self.logger.info("%s voting false on %s", self.hull.get_my_uri(),
                             message.sender)
            vote = False
        else: # both term and index proposals are acceptable, so vote yes
            self.last_vote = message.sender
            self.logger.info("%s voting true for candidate %s", self.hull.get_my_uri(), message.sender)
            vote = True
        await self.send_vote_response_message(message, votedYes=vote)
            
    async def term_expired(self, message):
        self.log.set_term(message.term)
        # process the message as normal
        return message

    async def leader_lost(self):
        await self.hull.start_campaign()
        
    async def send_vote_response_message(self, message, votedYes=True):
        vote_response = RequestVoteResponseMessage(sender=self.hull.get_my_uri(),
                                                   receiver=message.sender,
                                                   term=message.term,
                                                   prevLogIndex=self.log.get_last_index(),
                                                   prevLogTerm=self.log.get_last_term(),
                                                   vote=votedYes)
        await self.hull.send_response(message, vote_response)
        
    async def send_append_entries_response(self, message, new_records):
        if new_records is None:
            new_records = []
        append_response = AppendResponseMessage(sender=self.hull.get_my_uri(),
                                                receiver=message.sender,
                                                term=self.log.get_term(),
                                                entries=message.entries,
                                                results=new_records,
                                                prevLogIndex=message.prevLogIndex,
                                                prevLogTerm=message.prevLogTerm,
                                                myPrevLogIndex=self.log.get_last_index(),
                                                myPrevLogTerm=self.log.get_last_term())
        await self.hull.send_response(message, append_response)

    async def contact_checker(self):
        max_time = self.hull.get_leader_lost_timeout()
        e_time = time.time() - self.last_leader_contact
        if e_time > max_time:
            self.logger.debug("%s lost leader after %f", self.hull.get_my_uri(), e_time)
            await self.leader_lost()
            return
        # reschedule
        await self.run_after(self.hull.get_leader_lost_timeout(), self.contact_checker)
    
