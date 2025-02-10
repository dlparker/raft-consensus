import logging
from enum import Enum
from raftframe.messages.append_entries import AppendEntriesMessage, AppendResponseMessage
from raftframe.messages.request_vote import RequestVoteMessage, RequestVoteResponseMessage

class StateCode(str, Enum):

    """ A startup phase that allows app code to do things before 
    joining or after leaving cluster """
    paused = "PAUSED"

    """ Transitioning from one state to another """
    changing = "CHANGING"

    """ Follower state, as defined in raftframe protocol """
    follower = "FOLLOWER"

    """ Candidate state, as defined in raftframe protocol """
    candidate = "CANDIDATE"

    """ Leader state, as defined in raftframe protocol """
    leader = "LEADER"
    
class Substate(str, Enum):
    """ Before any connections """
    starting = "STARTING"

    """ Follower has not received timely leader contact """
    leader_lost = "LEADER_LOST"

    """ Follower, leader has called us at least once """
    joined = "JOINED"                  

    """ Follower, as of last call from leader, log is in sync """
    synced = "SYNCED"                 

    """ Follower, leader log is different than ours """
    out_of_sync = "OUT_OF_SYNC"

    """ Last call from leader synced new records """
    synced_prepare = "SYNCED_PREPARE"

    """ Last call from leader synced commit, no new records """
    synced_commit = "SYNCED_COMMIT"

    """ Candidate starting election """
    start_election = "start_election"

    """ Broadcasting vote """
    voting = "VOTING"

    """ Just got elected """
    became_leader = "BECAME_LEADER"
    
    """ Just sent term start (as leader) """
    sent_heartbeat = "SENT_HEARTBEAT"
    
    """ Just sent new log entries append (as leader) """
    sent_new_entries = "SENT_NEW_ENTRIES"
    
    """ Just sent log entry commit (as leader) """
    sent_commit = "SENT_COMMIT"


class BaseState:
    
    def __init__(self, hull, state_code):
        self.hull = hull
        self.state_code = state_code
        self.logger = logging.getLogger(__name__)
        self.substate = Substate.starting
        self.log = hull.get_log()
        self.routes = dict()
        code = AppendEntriesMessage.get_code()
        route = self.append_entries
        self.routes[code] = route
        code = AppendResponseMessage.get_code()
        route = self.append_entries_response
        self.routes[code] = route
        code = RequestVoteMessage.get_code()
        route = self.request_vote
        cod = RequestVoteResponseMessage.get_code()
        route = self.request_vote_response

    async def start(self):
        pass

    async def on_message(self, message):
        route = self.routes.get(message.get_code(), None)
        if route:
            return await route(message)

    async def append_entries(self, message):
        self.logger.warn('append_entries not implemented in the class "%", sending rejection',
                         self.__class__.__name__)
        await self.send_reject_append_response(message)

    async def append_entries_response(self, message):
        self.logger.warn('append_entries_response not implemented in the class "%", ignoring',
                         self.__class__.__name__)

    async def request_vote(self, message):
        self.logger.warn('request_vote not implemented in the class "%", sending rejection',
                         self.__class__.__name__)
        await self.send_reject_vote_response(message)

    async def request_vote_response(self, message):
        self.logger.warn('request_vote_response not implemented in the class "%", ignoring',
                         self.__class__.__name__)
    
    async def send_reject_append_response(self, message):
        data = dict(success=False,
                    last_index=self.log.get_commit_index(),
                    last_term=self.log.get_last_term())
        reply = AppendResponseMessage(message.receiver,
                                      message.sender,
                                      term=self.log.get_term(),
                                      data=data,
                                      prevLogTerm=self.log.get_last_term(),
                                      prevLogIndex=self.log.get_commit_index(),
                                      leaderCommit=self.log.get_commit_index())
        await self.hull.send_response(message, reply)

    async def send_reject_vote_response(self, message):
        data = dict(reponse=False)
        reply = RequestVoteResponseMessage(message.receiver,
                                           message.sender,
                                           term=self.log.get_term(),
                                           data=data,
                                           prevLogTerm=self.log.get_last_term(),
                                           prevLogIndex=self.log.get_commit_index(),
                                           leaderCommit=self.log.get_commit_index())
        await self.hull.send_response(message, reply)
