import random
import asyncio
from dataclasses import asdict
import logging
import traceback

from ..log.log_api import LogRec
from .voter import Voter
from .timer import Timer
from .candidate import Candidate
from ..messages.append_entries import AppendResponseMessage
from ..messages.log_pull import LogPullMessage


# Raft follower. Turns to candidate when it timeouts without receiving heartbeat from leader
class Follower(Voter):

    _type = "follower"
    
    def __init__(self, timeout=0.75, server=None, vote_at_start=False):
        Voter.__init__(self)
        self._timeout = timeout
        self._vote_at_start = vote_at_start
        # get this too soon and logging during testing does not work
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        self.election_timer = None
        self._leader_addr = None
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

    def on_log_pull_response(self, message):
        data = message.data
        log = self._server.get_log()
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
        
    def _do_sync_action(self, message):
        data = message.data
        leader_commit = data['leaderCommit']
        leader_last_rec_index = data["prevLogIndex"]
        leader_last_rec_term = data["prevLogIndex"]

        # The simplest case is that the local log and the leader log match. Look
        # for that first
        log = self._server.get_log()
        last_rec = log.read()
        if not last_rec:
            # local log is empty
            if leader_commit is None:
                # all logs are empty (ish), that's in sync, just return a hearbeat response
                return True
            # we got nothing, leader got something, get it all
            self.do_log_pull(message)
            return False
        else:
            if last_rec.index == leader_commit:
                if log.get_commit_index() == leader_commit -1:
                    # leader committed last record, and we didn't
                    # get the memo, so just apply it
                    log.commit(leader_commit)
                # logs in sync, just return a hearbeat response
                return True
        last_index = last_rec.index
        last_term = last_rec.term
        # Next simplest case is that our term is out of sync with the
        # leader. If so, we need to backout our local log records to match
        local_term = log.get_term()
        if local_term is not None and message.term < local_term:
            self.logger.warning("Leader says term is %d but we think %d, doing rollback",
                                message.term, local_term)
            self.do_rollback_to_leader(message)
            return False

        # Next simplest case is that we have the same term as leader, there is stuff
        # in both logs, and our last record matches the leader's commit index. The
        # leader might have an additional record that is not committed yet, so we
        # do not key off of leader's index
        if leader_commit is not None and last_index < leader_commit:
            self.do_log_pull(message)
            return False

        # Next case is we have records leader doesn't, possible if leader forced
        # from office during heal process. If so, then we need to rollback
        if leader_commit is not None and last_index > leader_commit:
            self.do_rollback_to_leader(message)
            return False

        # At this point we know that the term at leader matches ours,
        # that the last committed record at the leader is present in our
        # local log. Now check and see that we have the same commit index
        # locally. If our commit is behind, just update it. If it is
        # ahead,  some complex failover and restart scenario
        # got us out of sync and ahead of the leader, so
        # do a rollback.
        if leader_commit is not None and log.get_commit_index() > leader_commit:
            self.do_rollback_to_leader(message)
            return False
        if leader_commit is not None and log.get_commit_index() < leader_commit:
            log.commit(leader_commit)

        # At this point there are no more out of sync conditions to
        # detect, so just reply with a hearbeat
        return True
        
    def do_log_pull(self, message):
        # just tell the leader where we are and have him
        # send what we are missing

        log = self._server.get_log()
        last_rec = log.read()
        if last_rec is None:
            start_index = 0
        else:
            start_index = last_rec.index + 1
        self.logger.info("asking leader for log pull starting at %d", start_index)
        message = LogPullMessage(
            self._server.endpoint,
            message.sender,
            log.get_term(),
            {
                "start_index": start_index
            }
        )
        self._server.send_message_response(message)

    def do_rollback_to_leader(self, message):
        # figure out what the leader's actual
        # state is and roll back to that. 
        
        data = message.data
        leader_commit = data['leaderCommit']
        leader_last_rec_index = data["prevLogIndex"]
        leader_last_rec_term = data["prevLogIndex"]
        log = self._server.get_log()
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
            
    def on_append_entries(self, message):
        self.election_timer.reset()
        log = self._server.get_log()
        last_rec = log.read()
        data = message.data
        self._leader_addr = (data["leaderPort"][0], data["leaderPort"][1])

        self.logger.debug("on_append_entries message %s", message)
        if log.get_term() and message.term < log.get_term():
            self.send_response_message(message, votedYes=False)
            self.logger.info("rejecting message because sender term is less than mine %s", message)
            return
        leader_commit = data['leaderCommit']
        leader_last_rec_index = data["prevLogIndex"]
        leader_last_rec_term = data["prevLogIndex"]
        if last_rec is not None:
            if last_rec.index == leader_last_rec_index:
                # normal append condition, we are in sync
                # If leader is just telling us to commit, then
                # our local commit index will be one less than theirs
                local_commit = log.get_commit_index()
                if (local_commit is not None and leader_commit is not None
                    and local_commit == leader_commit - 1):
                    # just commit it, we have the record
                    log.commit(leader_commit)
                    return
        # log the records
        if len(data["entries"]) > 0:
            self.logger.debug("updating log with %d entries",
                              len(data["entries"]))
            for ent in data["entries"]:
                log.append([LogRec(term=ent['term'], user_data=ent['user_data']),])
                if ent['committed']:
                    log.commit(ent['index'])
            self.send_response_message(message)
            self.logger.info("Sent log update ack %s", message)
            return
        # we are not in sync, fix that
        self._do_sync_action(message)
    
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

    def on_term_start(self, message):
        self.election_timer.reset()
        log = self._server.get_log()
        self.logger.info("follower got term start: message.term = %s local_term = %s",
                         message.term, log.get_term())
        log.set_term(message.term)
        data = message.data
        self._leader_addr = (data["leaderPort"][0], data["leaderPort"][1])

    def on_client_command(self, message):
        self.dispose_client_command(message, self._server)

    def on_append_response(self, message): # pragma: no cover error
        self.logger.warning("follower unexpectedly got append response from %s",
                            message.sender)
    
    def on_vote_received(self, message): # pragma: no cover error
        log = self._server.get_log()
        self.logger.info("follower unexpectedly got vote: message.term = %d local_term = %d",
                         message.term, log.get_term())

