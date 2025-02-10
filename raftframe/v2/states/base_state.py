import asyncio
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
        self.logger = logging.getLogger(self.__class__.__name__)
        self.substate = Substate.starting
        self.log = hull.get_log()
        self.stopped = False
        self.async_handle = None
        self.routes = None
        self.build_routes()

    def build_routes(self):
        self.routes = dict()
        code = AppendEntriesMessage.get_code()
        route = self.on_append_entries
        self.routes[code] = route
        code = AppendResponseMessage.get_code()
        route = self.on_append_entries_response
        self.routes[code] = route
        code = RequestVoteMessage.get_code()
        route = self.on_vote_request
        self.routes[code] = route
        code = RequestVoteResponseMessage.get_code()
        route = self.on_vote_response
        self.routes[code] = route
        
    async def start(self):
        # child classes not required to have this method, but if they do,
        # they should call this one (i.e. super().start())
        self.stopped = False

    async def stop(self):
        # child classes not required to have this method, but if they do,
        # they should call this one (i.e. super().stop())
        self.stopped = True
        if self.async_handle:
            self.logger.info("%s canceling scheduled task", self.hull.get_my_uri())
            self.async_handle.cancel()

    async def after_runner(self, target):
        if self.stopped:
            return
        await target()
        
    async def run_after(self, delay, target):
        loop = asyncio.get_event_loop()
        self.async_handle = loop.call_later(delay,
                                            lambda target=target:
                                            asyncio.create_task(self.after_runner(target)))
        
    async def on_message(self, message):
        if message.term > self.log.get_term():
            self.logger.debug('received message from higher term, calling self.term_expired')
            res = await self.term_expired(message)
            if not res:
                self.logger.debug('self.term_expired said no further processing required')
                # no additional handling of message needed
                return None
        route = self.routes.get(message.get_code(), None)
        if route:
            return await route(message)

    async def on_append_entries(self, message):
        self.logger.warning('append_entries not implemented in the class "%s", sending rejection',
                         self.__class__.__name__)
        await self.send_reject_append_response(message)

    async def on_append_entries_response(self, message):
        self.logger.warning('append_entries_response not implemented in the class "%s", ignoring',
                         self.__class__.__name__)

    async def on_vote_request(self, message):
        self.logger.warning('request_vote not implemented in the class "%s", sending rejection',
                         self.__class__.__name__)
        await self.send_reject_vote_response(message)

    async def on_vote_response(self, message):
        self.logger.warning('request_vote_response not implemented in the class "%s", ignoring',
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
