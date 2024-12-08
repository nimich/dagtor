from src.logger import logger
import random
import time
from .task import Task, ExecutionState
from returns.result import safe


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
        self.depends_on = set()
        self.triggers = set()
        self.state = ExecutionState.RUNNING

    def add_dependency(self, task: Task):
        self.depends_on.add(task)

    def add_trigger(self, task: Task):
        self.triggers.add(task)

    def _execution_mock(self):
        duration = random.randint(1, 3)
        # if self.name == "task2":
        #     duration = 2
        # else:
        #     duration = 1
        time.sleep(duration)
        if random.randint(1, 3) > 2:
            raise Exception("Something failed")
        logger.info(f"Duration of {self.name} was {duration} sec")

    @safe
    def run(self) -> ExecutionState:
        try:
            logger.info(f"Starting Execution Task: {self.name}")
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
