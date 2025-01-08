from abc import abstractmethod
from typing import Protocol
from .execution_state import ExecutionState
from dagtor.state.data.pipeline import TaskExecution
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

    @abstractmethod
    def run(self) -> ExecutionState:
        pass

    # TODO ADD RENDER FUNCTION FOR HTML

    def add_dependency(self, task):
        """

        :param task:
        :return:
        """
        self.depends_on.add(task)

    def add_trigger(self, task):
        """

        :param task:
        :return:
        """
        self.triggers.add(task)

    def is_successful(self) -> bool:
        return self.state == ExecutionState.SUCCESS

    def dependencies_ended(self) -> bool:
        return all(dep.is_successful() for dep in self.depends_on)

    def from_dataclass(self, t: TaskExecution):
        self.pipeline_id = t.pipeline_id
        self.pipeline_execution_id = t.pipeline_execution_id
        self.id = t.id
        self.name = t.name
        self.state = ExecutionState[t.state]
        self.started = t.started
        self.ended = t.ended

    def to_dataclass(self) -> TaskExecution:
        return TaskExecution(
            pipeline_id=self.pipeline_id,
            pipeline_execution_id=self.pipeline_execution_id,
            id=self.id,
            name=self.name,
            state=self.state.to_string(),
            started=self.started,
            ended=self.ended,
        )
