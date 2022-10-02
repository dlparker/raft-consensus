from .base_state import State
from ..messages.request_vote import RequestVoteResponseMessage


# Base class for follower and candidate states
class Voter(State):

    def __init__(self):
        self._last_vote = None

    def on_vote_request(self, message):
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
        log = self._server.get_log()
        # get the last record in the log
        last_rec = log.read()
        if last_rec:
            last_index = last_rec.index
            last_term = last_rec.term
        else:
            # no log records yet
            last_index = None
            last_term = None
        vote = False
        if self._last_vote is None and last_index is None:
            vote = True
        elif (self._last_vote is None 
            and message.data["lastLogIndex"] >= last_rec.index):
            vote = True
        if vote:
            self._last_vote = message.sender
            self._send_vote_response_message(message)
        else:
            self._send_vote_response_message(message, votedYes=False)

        return self, None

    def _send_vote_response_message(self, message, votedYes=True):
        vote_response = RequestVoteResponseMessage(
            self._server.endpoint,
            message.sender,
            message.term,
            {"response": votedYes})
        self._server.send_message_response(vote_response)
