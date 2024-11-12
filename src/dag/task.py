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

    name: str = ""

    """
    Any task should execute something 
    """

    @abstractmethod
    def run(self) -> ExecutionState:
        pass

    @abstractmethod
    def is_successful(self) -> bool:
        pass

    @abstractmethod
    def has_dependency(self) -> bool:
        pass
