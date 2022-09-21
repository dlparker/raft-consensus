import logging
import asyncio
from ..messages.base_message import BaseMessage
from ..messages.response import ResponseMessage
from ..messages.status import StatusQueryResponseMessage

logger = logging.getLogger(__name__)


# abstract class for all states
class State(object):
    def set_server(self, server):
        self._server = server

    def on_message(self, message):
        """
        Called when receiving a message, then
        calls the corresponding methods based on states
        """

        _type = message.type

        if _type == BaseMessage.StatusQuery:
            return self.on_status_query(message)
        # If the message.term < currentTerm -> tell the sender to update term
        if (message.term > self._server._currentTerm):
            self._server._currentTerm = message.term
        elif (message.term < self._server._currentTerm):
            self._send_response_message(message, votedYes=False)
            return self, None
        if (_type == BaseMessage.AppendEntries):
            return self.on_append_entries(message)
        elif (_type == BaseMessage.RequestVote):
            return self.on_vote_request(message)
        elif (_type == BaseMessage.RequestVoteResponse):
            return self.on_vote_received(message)
        elif (_type == BaseMessage.Response):
            return self.on_response_received(message)

    def on_vote_request(self, message):
        """Called when there is a vote request"""

    def on_vote_received(self, message):
        """Called when this node receives a vote"""
        return self, None

    def on_append_entries(self, message):
        """Called when there is a request for this node to append entries"""

    def on_response_received(self, message):
        """Called when a response is sent back to the leader"""

    def on_client_command(self, message, client_port):
        """Called when there is a client request"""

    def on_status_query(self, message):
        """Called when there is a status query"""
        state_type = str(self._server._state)
        if state_type == "leader":
            leader_addr = self._server.endpoint
        elif state_type == "candidate":
            leader_addr = (-1,-1)
        else:
            leader_addr = self._server._state._leaderPort
        status_data = dict(state=state_type, leader=leader_addr)
        status_response = StatusQueryResponseMessage(
            self._server.endpoint,
            message.sender,
            self._server._currentTerm,
            status_data
        )
        asyncio.ensure_future(self._server.post_message(status_response))
        logger.debug("posted %s %s", status_response, status_response.__dict__)
        return self, None

    def _send_response_message(self, msg, votedYes=True):
        response = ResponseMessage(
            self._server.endpoint,
            msg.sender,
            msg.term,
            {
                "response": votedYes,
                "currentTerm": self._server._currentTerm,
            }
        )
        self._server.send_message_response(response)
