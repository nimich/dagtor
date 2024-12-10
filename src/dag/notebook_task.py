from src.logger import logger
import random
import time
from .task import Task
from .execution_state import ExecutionState
from returns.result import safe

from src.state.data import TaskExecution


class NotebookTask(Task):
    def __init__(
        self,
        name: str,
        execution_path: str = "",
        execution_config: str = "",
    ):
        self.name = name
        self.execution_path = execution_path
        self.execution_config = execution_config
        self.id = None
        self.pipeline_id = None
        self.pipeline_execution_id = None
        self.state = ExecutionState.SUBMITTED
        self.started = None
        self.ended = None
        self.depends_on = set()
        self.triggers = set()

    def add_dependency(self, task: Task):
        self.depends_on.add(task)

    def add_trigger(self, task: Task):
        self.triggers.add(task)

    def _execution_mock(self):
        duration = random.randint(3, 5)
        time.sleep(duration)
        if random.randint(1, 4) > 2:
            raise Exception("Something failed")
        logger.info(f"Duration of {self.name} was {duration} sec")

    @safe
    def run(self) -> ExecutionState:
        try:
            self._execution_mock()
            self.state = ExecutionState.SUCCESS
        except Exception as e:
            logger.error(f"Failed to Execution Task: {self.name}")
            self.state = ExecutionState.FAILURE
            raise e
        return self.state

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
