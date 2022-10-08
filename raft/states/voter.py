import logging
from .base_state import State
from ..messages.request_vote import RequestVoteResponseMessage


# Base class for follower and candidate states
class Voter(State):

    def __init__(self):
        self.last_vote = None
        # this will be overriden by child states, prolly, s'fine
        self.logger = logging.getLogger(__name__) 
        

    async def common_on_vote_request(self, message):
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
        if approve:
            self.last_vote = message.sender
            await self.send_vote_response_message(message, votedYes=True)
        else:
            self.logger.info("voting false")
            await self.send_vote_response_message(message, votedYes=False)
        return approve

    async def send_vote_response_message(self, message, votedYes=True):
        vote_response = RequestVoteResponseMessage(
            self.server.endpoint,
            message.sender,
            message.term,
            {"response": votedYes})
        await self.server.send_message_response(vote_response)
