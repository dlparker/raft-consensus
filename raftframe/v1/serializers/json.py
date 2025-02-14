from typing import Union
from dataclasses import asdict
import json
from raftframe.log.log_api import LogRec
from raftframe.messages.base_message import BaseMessage
from raftframe.messages.regy import get_message_registry
from raftframe.serializers.api import SerializerAPI

class JsonSerializer:

    @staticmethod
    def serialize_dict(user_dict: dict) -> Union[bytes, str]:
        return json.dumps(user_dict).encode('utf-8')

    @staticmethod
    def deserialize_dict(data: Union[bytes, str]) -> dict:
        if isinstance(data, bytes):
            tmpdata = data.decode('utf-8')
        elif isinstance(data, str):
            tmpdata = data
        else:
            raise Exception(f'data is {type(data)}')
        return json.loads(tmpdata)

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
        if isinstance(data, bytes):
            tmpdata = data.decode('utf-8')
        elif isinstance(data, str):
            tmpdata = data
        else:
            raise Exception(f'data is {type(data)}')
        message = json.loads(tmpdata)
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
        return json.dumps(data).encode('utf-8')
        
    @staticmethod
    def deserialize_logrec(data: Union[bytes, str]) -> LogRec:
        if isinstance(data, bytes):
            tmpdata = data.decode('utf-8')
        elif isinstance(data, str):
            tmpdata = data
        else:
            raise Exception(f'data is {type(data)}')
        rec_data = json.loads(tmpdata)
        return LogRec.from_dict(rec_data)
    

    
    
