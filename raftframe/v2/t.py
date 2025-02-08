#!/usr/bin/env python

# FIX THIS:
import sys
from pathlib import Path
source_dir = Path(__file__).resolve().parent
top_dir = source_dir.parent.parent
sys.path.append(str(top_dir))

from raftframe.v2.config.hull_config import LiveConfig, ClusterConfig, LocalConfig
from raftframe.v2.states.hull import Hull

        
if __name__=="__main__":

    import logging
    import threading
    import time
    import multiprocessing
    from logging.config import dictConfig
    import asyncio
    import traceback
    
    from raftframe.states.state_map import StateMap
    from raftframe.serializers.json import JsonSerializer
    from dev_tools.memory_log import MemoryLog
    from raftframe.messages.append_entries import AppendEntriesMessage, AppendResponseMessage
    from raftframe.messages.request_vote import RequestVoteMessage, RequestVoteResponseMessage

    class MPStateMap(StateMap):
        
        def __init__(self, *args, **kwargs):
            self.bt_server_name = kwargs.pop('bt_server_name')
            csns.status[self.bt_server_name] = manager.dict()
            super().__init__(*args, **kwargs)
        
        async def set_substate(self, state, substate):
            await super().set_substate(state, substate)
            csns.status[self.bt_server_name]['state_type'] = str(state)
            csns.status[self.bt_server_name]['substate'] = substate
            
    manager = multiprocessing.Manager()
    csns = manager.Namespace()
    csns.status = manager.dict()
    csns.control = manager.dict()

    async def main():
        
        logger = logging.getLogger(__name__)
        logger.info("bank teller server starting")
        state_map = MPStateMap(bt_server_name="foo",
                               timeout_basis=0.1)
        data_log = MemoryLog()
        loop = asyncio.get_running_loop()
        uri = "mcpy://1"
        logger.info('creating hull for uri %s', uri)
        cc = ClusterConfig(name=f"{uri}",
                           uri=uri,
                           other_nodes=["mcpy://2", "mcpy://3"])
        local_config = LocalConfig(working_dir='/tmp')
        live_config = LiveConfig(cluster=cc,
                                 log=data_log,
                                 local=local_config,
                                 comms=None,
                                 state_map=state_map)

        hull = Hull(live_config)
        assert hull.get_log() == data_log
        print(hull.state.state_code)
        ae = AppendEntriesMessage('foo', 'bar', 1, 'a', 1, 1, False)
        res = await hull.on_message(ae)
        print(res)
        ae_resp = AppendResponseMessage('foo', 'bar', 1, 'a', 1, 1, False)
        res = await hull.on_message(ae_resp)
        print(res)
        
    asyncio.run(main())
