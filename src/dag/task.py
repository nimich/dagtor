from enum import Enum
from abc import abstractmethod
from typing import Protocol


"""
"""


class ExecutionState(Enum):
    SUCCESS = 1
    FAILURE = 2
    SUBMITTED = 3
    RUNNING = 4


"""
Abstract Base class for Tasks
"""


class Task(Protocol):
    """ """

    depends_on: set
    triggers: set
    name: str = ""

    """
    Any task should execute something 
    """
    # TODO ADD MEMEBERS
    # TODO ADD RENDER FUNACTION FOR HTML

    @abstractmethod
    def run(self) -> ExecutionState:
        pass

    @abstractmethod
    def is_successful(self) -> bool:
        pass

    @abstractmethod
    def dependencies_ended(self) -> bool:
        pass
