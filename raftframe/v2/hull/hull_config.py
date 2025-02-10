"""
Configuration classes for setting up an instance of the class::`Server` class.
"""
from dataclasses import dataclass
from typing import Any, Type, Callable, Awaitable
import os
from raftframe.log.log_api import LogAPI
from raftframe.v2.comms.comms_api import CommsAPI
from raftframe.messages.base_message import BaseMessage
from raftframe.serializers.api import SerializerAPI

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
    working_dir: os.PathLike # where the server should run and place log files, data files, etc
    uri: Any          # unique identifier of this server
    leader_lost_timeout: float
    election_timeout_min: float
    election_timeout_max: float

@dataclass
class ClusterConfig:
    """
    Class used to supply details of the cluster configuration to the server code.


    Args:
        node_uris: 
            A list of addresses of the all nodes in the cluster
            in the same form as the uri in the LocalConfig. This server's
            uri is in there too.

    """
    node_uris: list # addresses of other nodes in the cluster

@dataclass
class LiveConfig:
    """
    Class used to provide configuration to :class:raftframe.`v2.servers.server.Server` in
    the form of instantiated classes that are ready to be used.

    Args:
        cluster: 
            A :class:`ClusterConfig` instance that defines a cluster
        local:
            A :class:`LocalConfig` instance that defines the local machine configuration
        log:
            An instance of a class that implments :class:`raftframe.log.log_api.LogAPI` and
            provides log record storage and access using some underlying storage 
            technique. The default implementation for this is :class:`raftframe.log.sqlite_log.SqliteLog`.
        message_sender:
            A callable that returns and awaitable (such as an async function or method) that
            takes an instance of BaseMessage class and sends it to the addressed recipient
        response_sender:
            A callable that returns and awaitable (such as an async function or method) that
            takes an instance of BaseMessage class that was originally delivered to the caller
            and an response, that arranges to send the response according to something in 
            either the response or the original message, or both
    """
    
    cluster: ClusterConfig
    local: LocalConfig
    log: LogAPI # actual, live object
    message_sender: Callable[[BaseMessage], Awaitable[Any]]
    response_sender: Callable[[BaseMessage, BaseMessage], Awaitable[Any]]

    
