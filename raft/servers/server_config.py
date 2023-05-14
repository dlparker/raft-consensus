"""
Configuration classes for setting up an instance of the class::`Server` class.
"""
from dataclasses import dataclass
from typing import Any, Type
import os
from raft.log.log_api import Log
from raft.app_api.app import App
from raft.comms.comms_api import CommsAPI
from raft.states.state_map import StateMap
from raft.serializers.api import SerializerAPI

@dataclass
class ModulesConfig:
    app: Type[App]
    log: Type[Log]
    comms: Type[CommsAPI]
    state_map: Type[StateMap]
    serializer: Type[SerializerAPI]

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
    name: str         # name of this node in cluster
    endpoint: Any     # address for use with the CommsAPI instance
    other_nodes: list # addresses of other nodes in the cluster

@dataclass
class LiveConfig:
    cluster: ClusterConfig
    local: LocalConfig
    app: App # actual, live object
    log: Log # actual, live object
    comms: CommsAPI # actual, live object
    state_map: StateMap # actual, live object
    serializer: SerializerAPI # actual, live object

    
