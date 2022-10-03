import abc
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional

# abstract class for logged interaction app code
class App(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def execute_command(self, command) -> Union[dict, None]:
        raise NotImplementedError



        
    
