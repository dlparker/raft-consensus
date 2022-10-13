import logging
from .base_state import State
from ..messages.request_vote import RequestVoteResponseMessage


# Base class for follower and candidate states
class Voter(State):

    def __init__(self):
        self.last_vote = None
        # this will be overriden by child states, prolly, s'fine
        self.logger = logging.getLogger(__name__) 

    async def send_vote_response_message(self, message, votedYes=True):
        vote_response = RequestVoteResponseMessage(
            self.server.endpoint,
            message.sender,
            message.term,
            {"response": votedYes})
        await self.server.send_message_response(vote_response)
