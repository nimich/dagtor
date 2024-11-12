import random
import time
from .task import Task, ExecutionState
from returns.result import safe


class NotebookTask(Task):
    def __init__(
        self,
        name: str,
        id: int = 0,
        execution_path: str = "",
        execution_config: str = "",
        depends_on: list[Task] = None,
    ):
        self.name = name
        self.id = id
        self.execution_path = execution_path
        self.execution_config = execution_config
        self.depends_on = depends_on

    execution_state = ExecutionState.SUBMITTED

    def _execution_mock(self):
        duration = random.randint(1, 2)
        time.sleep(duration)
        if random.randint(1, 2) > 1:
            raise Exception("Something failed")
        print(f"Duration of {self.name} was {duration} sec")

    @safe
    def run(self) -> ExecutionState:
        try:
            print(f"Executing NotebookTask: {self.name}")
            self._execution_mock()
            self.execution_result = ExecutionState.SUCCESS
        except Exception as e:
            print(f"Failed to execute NotebookTask: {self.name}")
            self.execution_result = ExecutionState.FAILURE
            raise e
        return self.execution_result

    def is_successful(self) -> bool:
        return self.execution_result == ExecutionState.SUCCESS

    def has_dependency(self) -> bool:
        for t in self.depends_on:
            if not t.is_successful():
                return False
        return True
