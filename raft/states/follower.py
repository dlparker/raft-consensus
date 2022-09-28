import random
import asyncio
from dataclasses import asdict
import logging

from .log_api import LogRec
from .voter import Voter
from .timer import Timer
from .candidate import Candidate


# Raft follower. Turns to candidate when it timeouts without receiving heartbeat from leader
class Follower(Voter):

    _type = "follower"
    
    def __init__(self, timeout=0.75, vote_at_start=False):
        Voter.__init__(self)
        self._timeout = timeout
        self._leader_addr = None
        self._vote_at_start = vote_at_start
        # get this too soon and logging during testing does not work
        self.logger = logging.getLogger(__name__)
        self.heartbeat_logger = logging.getLogger(__name__ + ":heartbeat")
        
    def __str__(self):
        return "follower"

    def get_leader_addr(self):
        return self._leader_addr
    
    def set_server(self, server):
        self._server = server
        interval = self.election_interval()
        self.election_timer = Timer(interval, self._start_election)
        self.election_timer.start()
        if self._vote_at_start:
            asyncio.get_event_loop().call_soon(self._start_election)

    def election_interval(self):
        return random.uniform(self._timeout, 2 * self._timeout)

    def _start_election(self):
        self.election_timer.stop()
        candidate = Candidate()
        self._server._state = candidate
        candidate.set_server(self._server)
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
            self._send_response_message(message, votedYes=False)
            self.logger.info("rejecting out of date state claim from  %s",
                             message.sender)
            return False

        log_tail = log.get_tail()
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
        self.heartbeat_logger.debug("log data %s", log_tail)
        if leader_commit != log_tail.commit_index:
            if leader_commit > log_tail.last_index:
                commit_reason = "partial"
                commit_index = log_tail.last_index
            else:
                commit_reason = "full"
                commit_index = leader_commit
            self.logger.info("commiting up to %d as %s catch up with leader",
                             commit_index, commit_reason)
            log.commit(commit_index)

        leader_last_rec_index = data["prevLogIndex"]
        if log_tail.last_index < leader_last_rec_index:
            # tell the leader we need more log records, the
            # leader has more than we do.
            self._send_response_message(message, votedYes=False)
            self.logger.info("asking leader for more log records")
            return False
        if log_tail.last_index == 0:
            # TODO: need to eliminate this branch by improving
            # the log api so that we don't have to do the current
            # hacky business of inserting a dummy record when the
            # log is created. 
            pass
        else:
            # Look at the leader's provided data for most recent
            # log record, make sure that ours is not more recent.
            # Not sure that this can happen, seems to violate
            # promise of protocol. TODO: see if this can be forced
            # in testing, or if it is dead code
            last_matching_log_rec = log.read(leader_last_rec_index)
            leader_log_term = data['prevLogTerm']
            if last_matching_log_rec.term > leader_log_term:
                target_index = min(log_tail.last_index,
                                   leader_last_rec_index)
                self.logger.warning("leader commit is behind follower commit")
                self.logger.warning("leader commit term is %d, follower is %d",
                                    leader_log_term,
                                    last_matching_log_rec.term)
                log.trim_after(leader_last_rec_index)
                log.commit(leader_commit)
                self._send_response_message(message, votedYes=False)
                return False
        if len(data["entries"]) > 0:
            self.logger.debug("updating log with %d entries",
                              len(data["entries"]))
            for ent in data["entries"]:
                log.append([LogRec(user_data=ent),],
                           ent['term'])
            tail = log.commit(leader_commit)
            self._send_response_message(message)
            self.logger.info("Sent log update ack %s", message)
            return False
        self.heartbeat_logger.debug("no action needed on heartbeat")
        return True
        
    def on_append_entries(self, message):
        # reset timeout
        self.election_timer.reset()
        log = self._server.get_log()
        log_tail = log.get_tail()

        data = message.data
        self._leader_addr = (data["leaderPort"][0], data["leaderPort"][1])

        if len(message.data["entries"]) != 0:
            self.logger.debug("on_append_entries message %s", message)
        else:
            self.heartbeat_logger.debug("heartbeat from %s", message.sender)
        if message.term < log.get_term():
            self._send_response_message(message, votedYes=False)
            self.logger.info("rejecting message because sender term is less than mine %s", message)
            return self, None
        if message.data != {}:

            # Check if leader is too far ahead in log
            if data['leaderCommit'] != log_tail.commit_index:
                # if so then we use the last index
                commit_index = min(int(data['leaderCommit']),
                                   log_tail.last_index)
                self.logger.info("commiting %d because %s != %s and last index = %s",
                            commit_index, data['leaderCommit'],
                            log_tail.commit_index,
                            log_tail.last_index)
                self.heartbeat_logger.debug("leader data %s", data)
                self.heartbeat_logger.debug("log data %s", log_tail)
                log.commit(commit_index)
            # If log is smaller than prevLogIndex -> not up-to-date
            if log_tail.last_index < data["prevLogIndex"]:
                # tell the leader we need more log records, the
                # leader has more than we do.
                self._send_response_message(message, votedYes=False)
                self.logger.debug("rejecting message because our index is %s and sender is %s",
                             log_tail.last_index, data["prevLogIndex"])
                self.logger.debug("rejected message %s %s", message,
                                  message.data)
                return self, None
            # this checking of the last index is due
            # to the hacky use of a dummy log initial
            # record, meaning the first real record
            # is at index 1.
            # TODO: fix this when fixing the hack
            last_rec = log.read(data['prevLogIndex'])
            if (last_rec and log_tail.last_index > 0
                and last_rec.term != data['prevLogTerm']):
                # somehow follower got ahead of leader
                # not sure if this is possible
                # TODO: experiement to see if this code ever
                # runs
                self.logger.error("Follower is ahead of leader!!!")
                self.logger.error("last record term = %d, msg prevLogTerm = %d",
                                  last_rec.term, data['prevLogTerm'])
                self.logger.error(asdict(log_tail))
                self.logger.error(asdict(last_rec))
                self.logger.error(str(data))
                log.trim_after(data['prevLogIndex'])
                self._send_response_message(message, votedYes=False)
                return self, None
            else:
                if len(data["entries"]) > 0:
                    self.logger.debug("accepting message on our index=%s and sender=%s, data=%s",
                                      log_tail.last_index,
                                      data["prevLogIndex"], data)
                    for ent in data["entries"]:
                        log.append([LogRec(user_data=ent),],
                                   ent['term'])
                    tail = log.commit(data['leaderCommit'])
                    self.logger.info("commit %s from %s", tail, message.data)
                    self._send_response_message(message)
                    self.logger.info("Sent log saved on %s", message)
                    return self, None
                else:
                    #self.logger.debug("heartbeat")
                    self._send_response_message(message)
                    return self, None
            # TODO: cleanup the logic in this code, this send
            # is needed but it is also redundant in depending on the
            # branching above
            if len(data["entries"]) > 0:
                self.logger.debug("sending log update ack message to %s",
                                  message.receiver)
            self._send_response_message(message)
            return self, None
        else:
            return self, None

    def on_client_command(self, command, client_port):
        message = {
            'command': command,
            'client_port': client_port,
        }
        asyncio.ensure_future(self._server.post_message(message))
        return True

    def on_vote_received(self, message):
        log = self._server.get_log()
        self.logger.info("follower got vote: message.term = %d local_term = %d",
                         message.term, log.get_term())

    def on_response_received(self, message):
        raise NotImplementedError
    
    
