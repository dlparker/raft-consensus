import random
import asyncio
from dataclasses import asdict
import logging

from .log_api import LogRec
from .voter import Voter
from .timer import Timer
from .candidate import Candidate
logger = logging.getLogger(__name__)


# Raft follower. Turns to candidate when it timeouts without receiving heartbeat from leader
class Follower(Voter):

    def __init__(self, timeout=0.75, vote_at_start=False):
        Voter.__init__(self)
        self._timeout = timeout
        self._leaderPort = None
        self._vote_at_start = vote_at_start
        # get this too soon and logging during testing does not work
        self.logger = logging.getLogger(__name__)
        
    def __str__(self):
        return "follower"
    
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

    def on_append_entries(self, message):
        # reset timeout
        self.election_timer.reset()

        if message.term < self._server._currentTerm:
            self._send_response_message(message, votedYes=False)
            self.logger.debug("rejecting message because sender term is less than mine %s", message)
            return self, None
        if message.data != {}:
            log = self._server.get_log()
            log_tail = log.get_tail()
            data = message.data
            self._leaderPort = data["leaderPort"]

            # Check if leader is too far ahead in log
            if data['leaderCommit'] != log_tail.commit_index:
                # if so then we use the last index
                commit_index = min(int(data['leaderCommit']),
                                   log_tail.last_index)
                logger.info("commiting %d because %s != %s and last index = %s",
                            commit_index, data['leaderCommit'],
                            log_tail.commit_index,
                            log_tail.last_index)
                log.commit(commit_index)
            # If log is smaller than prevLogIndex -> not up-to-date
            if log_tail.last_index < data["prevLogIndex"]:
                # tell the leader we need more log records, the
                # leader has more than we do.
                self._send_response_message(message, votedYes=False)
                self.logger.debug("rejecting message because our index is %s and sender is %s",
                             len(log)-1, data["prevLogIndex"])
                return self, None
            last_rec = log.read(log_tail.last_index)
            if (last_rec and log_tail.last_index > 0
                and last_rec.term != data['prevLogTerm']):
                # somehow follower got ahead of leader
                # not sure if this is possible
                # TODO: experiement to see if this code ever
                # runs
                logger.error("Follower is ahead of leader!!!")
                logger.error(asdict(log_tail))
                logger.error(str(data))
                log.trim_after(data['prevLogIndex'])
                self._send_response_message(message, votedYes=False)
                return self, None
            else:
                if len(data["entries"]) > 0:
                    self.logger.debug("accepting message on our index=%s and sender=%s, data=%s",
                                 tail.last_index, data["prevLogIndex"], data)
                    for ent in data["entries"]:
                        log.append([LogRec(user_data=ent),],
                                   data['prevLogTerm'])
                    tail = log.commit()
                    logger.info("commit %s from %s", tail, message.data)
                    self._send_response_message(message)
                else:
                    logger.debug("heartbeat")
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
        logger.info("follower got vote: message.term = %d local_term = %d",
                    message.term, self._server._currentTerm)

    def on_response_received(self, message):
        raise NotImplementedError
    
    
