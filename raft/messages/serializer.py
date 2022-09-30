from .append_entries import AppendEntriesMessage, AppendResponseMessage
from .request_vote import *
from .status import StatusQueryMessage, StatusQueryResponseMessage
from .command import ClientCommandMessage, ClientCommandResultMessage
from .heartbeat import HeartbeatMessage, HeartbeatResponseMessage
from .termstart import TermStartMessage, TermStartResponseMessage

import msgpack


class Serializer:
    @staticmethod
    def serialize(message):
        data = {
            'type': message._type,
            'sender': message.sender,
            'receiver': message.receiver,
            'data': message.data,
            'term': message.term,
            'original_sender': message.original_sender,
        }
        return msgpack.packb(data, use_bin_type=True)

    @staticmethod
    def deserialize(data):
        message = msgpack.unpackb(data, use_list=True, encoding='utf-8')
        mtype = message['type']
        args = [message['sender'],
                message['receiver'],
                message['term'],
                message['data']]

        # TODO: these matches should be done against the type
        # string in the class defs, not literals. Prolly should
        # have a dispatch table method in the BaseMessage class
        # that does this automatically, initialized by some
        # register function. Currently you have to know and remember
        # to edit serveral locations to keep things in sync when adding
        # or changing.
        if mtype == 'heartbeat':
            return HeartbeatMessage(*args)
        elif mtype == 'heartbeat_response':
            return HeartbeatReponseMessage(*args)
        elif mtype == 'term_start':
            return TermStartMessage(*args)
        elif mtype == 'term_start_response':
            return TermStartReponseMessage(*args)
        elif mtype == 'append_entries':
            return AppendEntriesMessage(*args)
        elif mtype == 'append_response':
            return AppendResponseMessage(*args)
        elif mtype == 'request_vote':
            return RequestVoteMessage(*args)
        elif mtype == 'request_vote_response':
            return RequestVoteResponseMessage(*args)
        elif mtype == 'status_query':
            return StatusQueryMessage(*args)
        elif mtype == 'status_query_response':
            return StatusQueryResponseMessage(*args)
        elif mtype == 'command':
            args.append(message['original_sender'])
            return ClientCommandMessage(*args)
        elif mtype == 'command_result':
            return ClientCommandResultMessage(*args)
        raise Exception(f"No code for provided message type {mtype}")

    @staticmethod
    def serialize_client(message, client_port):
        data = {
            'command': message,
            'client_port': client_port,
        }
        return msgpack.packb(data, use_bin_type=True)

    @staticmethod
    def deserialize_client(data):
        message = msgpack.unpackb(data, use_list=True, encoding='utf8')
        return message
