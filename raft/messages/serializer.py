from .append_entries import AppendEntriesMessage
from .request_vote import *
from .response import ResponseMessage
from .status import StatusQueryMessage, StatusQueryResponseMessage
from .command import ClientCommandMessage, ClientCommandResultMessage

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
        if mtype == 'append_entries':
            return AppendEntriesMessage(message['sender'],
                                        message['receiver'],
                                        message['term'],
                                        message['data'])
        elif mtype == 'request_vote':
            return RequestVoteMessage(message['sender'],
                                      message['receiver'],
                                      message['term'],
                                      message['data'])
        elif mtype == 'request_vote_response':
            return RequestVoteResponseMessage(message['sender'],
                                      message['receiver'],
                                      message['term'],
                                      message['data'])
        elif mtype == 'response':
            return ResponseMessage(message['sender'],
                                      message['receiver'],
                                      message['term'],
                                      message['data'])
        elif mtype == 'status_query':
            return StatusQueryMessage(message['sender'],
                                      message['receiver'],
                                      message['term'],
                                      message['data'])
        elif mtype == 'status_query_response':
            return StatusQueryResponseMessage(message['sender'],
                                      message['receiver'],
                                      message['term'],
                                      message['data'])

        elif mtype == 'command':
            return ClientCommandMessage(message['sender'],
                                        message['receiver'],
                                        message['term'],
                                        message['data'],
                                        message['original_sender'])
        elif mtype == 'command_result':
            return ClientCommandResultMessage(message['sender'],
                                      message['receiver'],
                                      message['term'],
                                      message['data'])

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
