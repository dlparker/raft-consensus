from .regy import get_message_registry

import msgpack


class Serializer:
    @staticmethod
    def serialize(message):
        data = {
            'code': message.code,
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
        mcode = message['code']
        args = [message['sender'],
                message['receiver'],
                message['term'],
                message['data']]
        if "original_sender" in message:
            os = message['original_sender']
            if os:
                args.append(os)

        regy = get_message_registry()
        cls =  regy.get_message_class(mcode)
        return cls(*args)
    
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
