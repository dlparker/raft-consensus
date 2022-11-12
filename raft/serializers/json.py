from typing import Union
from dataclasses import asdict
import json
from raft.log.log_api import LogRec
from raft.messages.base_message import BaseMessage
from raft.messages.regy import get_message_registry
from raft.serializers.api import SerializerAPI

class JsonSerializer:

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
        return json.dumps(data).encode('utf-8')

    @staticmethod
    def deserialize_message(data: Union[bytes, str]) -> BaseMessage:
        message = json.loads(data.decode('utf-8'))
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
    

    def serialize_logrec(rec: LogRec) -> Union[bytes, str]:
        data = asdict(rec)
        return json.dumps(data).encode('utf-8')
        
    def deserialize_logrec(data: Union[bytes, str]) -> LogRec:
        rec_data = json.loads(data.decode('utf-8'))
        return LogRec.from_dict(rec_data)
    
    def serialize_dict(user_dict: dict) -> Union[bytes, str]:
        return json.dumps(user_dict).encode('utf-8')

    def deserialize_dict(data: Union[bytes, str]) -> dict:
        return json.loads(data.decode('utf-8'))

    
    
