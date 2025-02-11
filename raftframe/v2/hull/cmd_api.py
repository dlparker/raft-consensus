import abc
from dataclasses import dataclass
from typing import List, Any

# abstract class for command processing, kind of a domain specific language
class DSLAPI(metaclass=abc.ABCMeta):
    """
    Abstract base class that functions as an interface definition for 
    implmentations of a processing module that callers build that allows
    raft operations to trigger user defined commands once the raft 
    algorithm has determined that the command is safe to commit. This
    is explained in the raft paper. The basic theory is that the commands
    are submitted to the raft library which ensures that there is a consensus
    among the servers in the cluster that the command will be exectuted. The 
    paper describes the command as triggering a transition in the application's
    state machine. The command should be provided in string form to the 
    raft library in the form of a string. When the library determines that
    the command can be performed, it will call the processing module with the
    string, which is responsible for turning that into application activity.
    Note that the string must make sense to all the servers in the cluster, and
    therefore cannot encode references ephemeral local state, unless everything
    is fully encoded. Also, the receiving server my have rebooted at anytime 
    between two commands, so it might be helpful to think of this as a
    mechanism that replays state transitions that are encoded in strings.
    """
    
    @abc.abstractmethod
    async def process_command(self, command: str) -> List[Any]:# pragma: no cover abstract
        raise NotImplementedError

