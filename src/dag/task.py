from abc import ABC, abstractmethod
from returns.result import Result, Success, Failure

"""
Abstract Base class for Tasks
"""


class Task(ABC):
    @abstractmethod
    def run() -> Result[Success, Failure]:
        pass
