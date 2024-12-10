from enum import Enum


class ExecutionState(Enum):
    SUCCESS = 1
    FAILURE = 2
    SUBMITTED = 3
    RUNNING = 4

    def to_string(self) -> str:
        """
        Return enum value as sting
        :return:
        """
        return self.name
