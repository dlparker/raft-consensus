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

    
