import random
import asyncio
from dataclasses import asdict
import logging

from .log_api import LogRec
from .voter import Voter
from .timer import Timer
from .candidate import Candidate
from ..messages.append_entries import AppendResponseMessage


# Raft follower. Turns to candidate when it timeouts without receiving heartbeat from leader
class Follower(Voter):

    _type = "follower"
    
    def __init__(self, timeout=0.75, server=None, vote_at_start=False):
        Voter.__init__(self)
        self._timeout = timeout
        self._leader_addr = None
        self._vote_at_start = vote_at_start
        # get this too soon and logging during testing does not work
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        self.election_timer = None
        self._server = None
        if server:
            self.set_server(server)

    def __str__(self):
        return "follower"

    def get_leader_addr(self):
        return self._leader_addr
    
    def set_server(self, server):
        if self._server:
            return
        self._server = server
        server.set_state(self)
        self.logger.debug("called server set_state")
        interval = self.election_interval()
        self.election_timer = self._server.get_timer("follower-election",
                                                     interval,
                                                     self._start_election)
        self.election_timer.start()
        if self._vote_at_start:
            asyncio.get_event_loop().call_soon(self._start_election)

    def election_interval(self):
        return random.uniform(self._timeout, 2 * self._timeout)

    def _start_election(self):
        self.election_timer.stop()
        candidate = Candidate(self._server)
        return candidate, None

    def on_heartbeat(self, message):
        # reset timeout
        self.election_timer.reset()
        self.heartbeat_logger.debug("heartbeat from %s", message.sender)
        data = message.data
        self._leader_addr = (data["leaderPort"][0], data["leaderPort"][1])
        if self._do_sync_action(message):
            self.on_heartbeat_common(message)
            self.heartbeat_logger.debug("heartbeat reply send")

    def _do_sync_action(self, message):
        data = message.data
        # If the sender is still claiming an earlier
        # term, then we reject the state claim. Not sure
        # if this is actually possible, unless maybe there
        # is an old message in a queue somewhere that gets
        # sent after an election completes.
        log = self._server.get_log()
        if message.term < log.get_term():
            self.send_response_message(message, votedYes=False)
            self.logger.info("rejecting out of date state claim from  %s",
                             message.sender)
            return False
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        # If the leader has committed something newer than our
        # latest commit record, then we want to catch up as much
        # as we can. That means that our most recent record should
        # be committed unless it is more recent than the leader's
        # commit index. In that case we should commit up to the
        # leader's index.
        # verbose for clarity
        commit_reason = None
        commit_index = None
        leader_commit = data['leaderCommit']
        self.heartbeat_logger.debug("leader data %s", data)
        self.heartbeat_logger.debug("log data %s", last_rec)
        if leader_commit != log.get_commit_index():
            if leader_commit > last_index:
                commit_reason = "partial"
                commit_index = last_index
            else:
                commit_reason = "full"
                commit_index = leader_commit
            self.logger.info("commiting up to %d as %s catch up with leader",
                             commit_index, commit_reason)
            log.commit(commit_index)

        leader_last_rec_index = data["prevLogIndex"]
        if last_index != leader_last_rec_index:
            # tell the leader we need more log records, the
            # leader has more than we do.
            self.send_response_message(message, votedYes=False)
            self.logger.info("asking leader for more log records")
            return False
        # Look at the leader's provided data for most recent
        # log record, make sure that ours is not more recent.
        # Not sure that this can happen, seems to violate
        # promise of protocol. TODO: see if this can be forced
        # in testing, or if it is dead code
        # First check to see that we don't have the empty logs situation
        if leader_last_rec_index and last_index:
            last_matching_log_rec = log.read(leader_last_rec_index)
            leader_log_term = data['prevLogTerm']
            if last_matching_log_rec.term > leader_log_term:
                target_index = min(last_index,
                                   leader_last_rec_index)
                self.logger.warning("leader commit is behind follower commit")
                self.logger.warning("leader commit term is %d, follower is %d",
                                    leader_log_term,
                                    last_matching_log_rec.term)
                log.trim_after(leader_last_rec_index)
                log.commit(leader_commit)
                self.send_response_message(message, votedYes=False)
                return False
        
        if len(data["entries"]) > 0:
            self.logger.debug("updating log with %d entries",
                              len(data["entries"]))
            for ent in data["entries"]:
                log.append([LogRec(term=ent['term'], user_data=ent),])
            tail = log.commit(leader_commit)
            self.send_response_message(message)
            self.logger.info("Sent log update ack %s", message)
            return False
        self.heartbeat_logger.debug("no action needed on heartbeat")
        return True
        
    def on_append_entries(self, message):
        self.election_timer.reset()
        log = self._server.get_log()
        last_rec = log.read()
        data = message.data
        self._leader_addr = (data["leaderPort"][0], data["leaderPort"][1])

        self.logger.debug("on_append_entries message %s", message)
        if message.term < log.get_term():
            self.send_response_message(message, votedYes=False)
            self.logger.info("rejecting message because sender term is less than mine %s", message)
            return 
        self._do_sync_action(message)
        
    def on_client_command(self, command, client_port):
        self.logger.info("follower got client command %s, discarding", command)
        return True
    
    def send_response_message(self, msg, votedYes=True):
        log = self._server.get_log()
        data = {
            "response": votedYes,
            "currentTerm": log.get_term(),
        }
        data.update(msg.data)
        response = AppendResponseMessage(
            self._server.endpoint,
            msg.sender,
            msg.term,
            data
        )
        self._server.send_message_response(response)
        logger = logging.getLogger(__name__)
        logger.info("sent response to %s term=%d %s",
                        response.receiver, response.term, data)

    def on_vote_received(self, message):
        log = self._server.get_log()
        self.logger.info("follower got vote: message.term = %d local_term = %d",
                         message.term, log.get_term())

    def on_term_start(self, message):
        self.election_timer.reset()
        log = self._server.get_log()
        self.logger.info("follower got term start: message.term = %d local_term = %d",
                         message.term, log.get_term())
        log.set_term(message.term)
        data = message.data
        self._leader_addr = (data["leaderPort"][0], data["leaderPort"][1])

    def on_append_response(self, message):
        self.logger.warning("follower unexpectedly got append response from %s",
                            message.sender)

    
    
