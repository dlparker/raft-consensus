"""
Configuration classes for setting up an instance of the class::`Server` class.
"""
from dataclasses import dataclass
from typing import Any, Type, Callable, Awaitable
import os
from raftframe.v2.log.log_api import LogAPI
from raftframe.v2.messages.base_message import BaseMessage

@dataclass
class LocalConfig:
    """
    Class used to supply details of the runtime configuration on the local machine to 
    the server code. 

    Args:
        working_dir:
            The location for the runtime to use as a working directory for output files 
            and the like
        uri: 
            Unique identifyer for this server, either directly
            serving as a comms endpoint or translatable to one
    """
    working_dir: os.PathLike # where the server should run and place log files, data files, etc
    uri: Any          # unique identifier of this server

@dataclass
class ClusterConfig:
    """
    Class used to supply details of the cluster configuration to the server code.


    Args:
        node_uris: 
            A list of addresses of the all nodes in the cluster
            in the same form as the uri in the LocalConfig. This server's
            uri is in there too.
        heartbeat_period:
            Leader sends a heartbeat message if it hasn't sent other messages 
            in this amount of time (float seconds)
        leader_lost_timeout:
            if no leader messages in longer than this time, start an election
        election_timeout_min:
            start another election if no leader elected in a random
            amount of time bounded by election_timeout_min and election_timeout_max,
            raft paper suggests range of 150 to 350 milliseconds
        election_timeout_max:
            start another election if no leader elected in a random
            amount of time bounded by election_timeout_min and election_timeout_max,
            raft paper suggests range of 150 to 350 milliseconds
    """
    node_uris: list # addresses of other nodes in the cluster
    heartbeat_period: float
    leader_lost_timeout: float
    election_timeout_min: float
    election_timeout_max: float

    
