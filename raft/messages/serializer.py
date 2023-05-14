import msgpack
from raft.messages.regy import get_message_registry

class Serializer:
    @staticmethod
    def serialize(message, msg_number=-1):
        data = {
            'code': message.code,
            'sender': message.sender,
            'receiver': message.receiver,
            'data': message.data,
            'term': message.term,
            'msg_number': message.msg_number,
        }
        for key in message.get_extra_fields():
            data[key] = getattr(message, key)
        return msgpack.packb(data, use_bin_type=True)

    @staticmethod
    def deserialize(data):
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
        if "msg_number" in message:
            res.set_msg_number(message['msg_number'])
        return res
    
