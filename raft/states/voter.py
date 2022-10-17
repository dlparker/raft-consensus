import logging
from .base_state import State
from ..messages.request_vote import RequestVoteResponseMessage

class Voter(State):

    def __init__(self, server, my_type):
        super().__init__(server, my_type)
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
