from raftframe.v2.states.base_state import StateCode, Substate, BaseState
from raftframe.v2.states.context import RaftContext

class Follower(BaseState):

    def __init__(self, hull):
        super().__init__(hull, StateCode.follower)
        self.last_vote = None
        self.last_vote_term = -1

    async def append_entries(self, message):
        print('follower in append_entries')
        return RaftContext()

    async def on_vote_request(self, message):
        last_index = self.log.get_last_index()
        last_term = self.log.get_last_term()
        # If this node has not voted, or voted in a previous election,
        # and if lastLogIndex in message is not earlier than our local 
        # log index then we agree that the sender's claim to be leader
        # can stand, so we vote yes.
        if message.prevLogIndex >= last_index:
            if self.last_vote is None or self.last_vote_term < message.term:
                approve = True
                self.last_vote = message.sender
                self.last_vote_term = message.term
                self.logger.info("voting true")
                await self.send_vote_response_message(message, votedYes=True)
        else:
            self.logger.info("voting false on message %s %s",
                             message, message.data)
            self.logger.info("my last vote = %s, index %d, last term %d",
                             self.last_vote, last_index, last_term)
            await self.send_vote_response_message(message, votedYes=False)
        if message.term > self.log.get_term():
            # regardless, increase the term to no less than that of the
            # sender
            self.log.set_term(message.term)
        return True

    async def send_vote_response_message(self, message, votedYes=True):
        vote_response = RequestVoteResponseMessage(
            self.hull.get_my_uri(),
            message.sender,
            message.term,
            {"response": votedYes})

