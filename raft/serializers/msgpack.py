from typing import Union
import msgpack
from raft.messages.base_message import BaseMessage
from raft.messages.regy import get_message_registry
from raft.serializers.api import SerializerAPI

class MsgpackSerializer:
    @staticmethod
    def serialize_message(message: BaseMessage) -> Union[bytes, str]:
        data = {
            'code': message.code,
            'sender': message.sender,
            'receiver': message.receiver,
            'data': message.data,
            'term': message.term,
        }
        for key in message.get_extra_fields():
            data[key] = getattr(message, key)
        return msgpack.packb(data, use_bin_type=True)

    @staticmethod
    def deserialize_message(data: Union[bytes, str]) -> BaseMessage:
        message = msgpack.unpackb(data, use_list=True, encoding='utf-8')
        mcode = message['code']
        regy = get_message_registry()
        cls =  regy.get_message_class(mcode)
        args = [message['sender'],
                message['receiver'],
                message['term'],
                message['data']]
        
        for key in cls.get_extra_fields():
            args.append(message[key])
            
        res = cls(*args)
        return res
    
