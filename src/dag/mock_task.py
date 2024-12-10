from src.logger import logger
import random
import time
from .task import Task
from .execution_state import ExecutionState
from returns.result import safe


class MockTask(Task):
    def __init__(self, name: str):
        self.name = name
        self.id = None
        self.pipeline_id = None
        self.pipeline_execution_id = None
        self.state = ExecutionState.SUBMITTED
        self.started = None
        self.ended = None
        self.depends_on = set()
        self.triggers = set()

    # This could not be added in Task protocol as they require tasks

    def _execution_mock(self):
        """
        delay for random ammount of time and produce errprs with a probability
        :return:
        """
        duration = random.randint(3, 5)
        time.sleep(duration)
        if random.randint(1, 4) > 3:
            raise Exception("Something failed")
        logger.info(f"Duration of {self.name} was {duration} sec")

    @safe
    def run(self) -> ExecutionState:
        try:
            self._execution_mock()
            self.state = ExecutionState.SUCCESS
        except Exception as e:
            self.state = ExecutionState.FAILURE
            raise e
        return self.state
