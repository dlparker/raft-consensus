"""
Configuration classes for setting up an instance of the class::`Server` class.
"""
from dataclasses import dataclass
from typing import Any, Type
import os
from raftframe.log.log_api import LogAPI
from raftframe.app_api.app import AppAPI
from raftframe.comms.comms_api import CommsAPI
from raftframe.states.state_map import StateMap
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

    """
    working_dir: os.PathLike # where the server should run and place log files, data files, etc

@dataclass
class ClusterConfig:
    """
    Class used to supply details of the cluster configuration to the server code.


    Args:
        name:
            The name of this node in the cluster map
        endpoint: 
            A specification of the COMMS endpoint for this server 
            in the form needed by the configured COMSS module.
        other_nodes: 
            A list of addresses of the other nodes in the cluster
            in the form needed by the configured COMSS module.

    """
    name: str         # name of this node in cluster
    endpoint: Any     # address for use with the CommsAPI instance
    other_nodes: list # addresses of other nodes in the cluster

@dataclass
class LiveConfig:
    """
    Class used to provide configuration to :class:raftframe.`servers.server.Server` in
    the form of instantiated classes that are ready to be used.

    Args:
        cluster: 
            A :class:`ClusterConfig` instance that defines a cluster
        local:
            A :class:`LocalConfig` instance that defines the local machine configuration
        app:
            An instance of a class that implemenents :class:`raftframe.app_api.app.AppAPI`
            and provides an interface between the 'user' code and the RaftFrame code.
        log:
            An instance of a class that implments :class:`raftframe.log.log_api.LogAPI` and
            provides log record storage and access using some underlying storage 
            technique. The default implementation for this is :class:`raftframe.log.sqlite_log.SqliteLog`.
        comms:
            An instance of a class that implments :class:`raftframe.comms.comss_api.CommsAPI` and
            provides a message transport mechanism. The default implementation for this is 
            :class:`raftframe.comms.udp.UDPComms`.
        state_map:
            An instance of a class that implments :class:`raftframe.states.state_map.StateMap`.
            The default implementation for this is :class:`raftframe.states.state_map.StandardStateMap`.
        serializer:
            An instance of a class that implments :class:`raftframe.serializers.api.SerializerAPI`.
            The default implementation for this is :class:`raftframe.serializers.msgpack.MsgpackSerializer`.
    """
    
    cluster: ClusterConfig
    local: LocalConfig
    app: AppAPI # actual, live object
    log: LogAPI # actual, live object
    comms: CommsAPI # actual, live object
    state_map: StateMap # actual, live object
    serializer: SerializerAPI # actual, live object

    
