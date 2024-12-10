from abc import abstractmethod
from typing import Protocol
from .execution_state import ExecutionState
from src.state.data.pipeline import TaskExecution
from datetime import datetime


class Task(Protocol):
    """
    Protocol for tasks
    """

    id: int
    name: str
    pipeline_id: int
    pipeline_execution_id: int
    state: ExecutionState
    started: datetime
    ended: datetime
    depends_on: set = set()
    triggers: set = set()

    # TODO ADD RENDER FUNCTION FOR HTML

    @abstractmethod
    def run(self) -> ExecutionState:
        pass

    @abstractmethod
    def is_successful(self) -> bool:
        pass

    @abstractmethod
    def dependencies_ended(self) -> bool:
        pass

    @abstractmethod
    def from_dataclass(self, t: TaskExecution):
        pass

    @abstractmethod
    def to_dataclass(self) -> TaskExecution:
        pass
