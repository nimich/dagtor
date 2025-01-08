import time

from databricks.sdk import WorkspaceClient
from returns.result import safe

from dagtor.logger import logger
from .execution_state import ExecutionState
from .task import Task


class DatabrickJobTask(Task):
    def __init__(self, name: str, job_id: int, host: str, token: str):
        self.name = name
        self.job_id = job_id
        self.host = host
        self.token = token
        self.id = None
        self.pipeline_id = None
        self.pipeline_execution_id = None
        self.state = ExecutionState.SUBMITTED
        self.started = None
        self.ended = None
        self.depends_on = set()
        self.triggers = set()

        self.w = WorkspaceClient(host=self.host, token=self.token)

    @safe
    def run(self) -> ExecutionState:
        try:
            run_response = self.w.jobs.run_now(job_id=self.job_id)
            get_run_response = self.w.jobs.get_run(run_id=run_response.run_id)
            run_id = get_run_response.run_id
            logger.debug(f"Job run id: {run_id}")
            lifecycle_state = "RUNNING"

            while lifecycle_state in ["RUNNING", "SUBMITTED"]:
                time.sleep(4)
                get_run_response = self.w.jobs.get_run(run_id=run_id)
                lifecycle_state = get_run_response.state.life_cycle_state.name
                logger.debug(f"{self.name}: Job state is {lifecycle_state}")

            result_state = self.w.jobs.get_run(run_id=run_id).state.result_state.name

            if result_state == "SUCCESS":
                self.state = ExecutionState.SUCCESS
            else:
                self.state = ExecutionState.FAILURE
        except Exception as e:
            self.state = ExecutionState.FAILURE
            raise e
        return self.state
