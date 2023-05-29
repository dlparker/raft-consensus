from typing import Union
from dataclasses import asdict
import msgpack
from raftframe.messages.base_message import BaseMessage
from raftframe.log.log_api import LogRec
from raftframe.messages.regy import get_message_registry
from raftframe.serializers.api import SerializerAPI

class MsgpackSerializer(SerializerAPI):

    @staticmethod
    def serialize_dict(user_dict: dict) -> Union[bytes, str]:
        return msgpack.packb(user_dict, use_bin_type=True)

    @staticmethod
    def deserialize_dict(data: Union[bytes, str]) -> dict:
        return msgpack.unpackb(data, use_list=True, encoding='utf-8')

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

    @staticmethod
    def serialize_logrec(rec: LogRec) -> Union[bytes, str]:
        data = asdict(rec)
        return msgpack.packb(data, use_bin_type=True)
        
    @staticmethod
    def deserialize_logrec(data: Union[bytes, str]) -> LogRec:
        rec_data = msgpack.unpackb(data, use_list=True, encoding='utf-8')
        return LogRec.from_dict(rec_data)
    
    
