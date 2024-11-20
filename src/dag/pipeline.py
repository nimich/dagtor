from typing import List
import concurrent.futures

from .task import Task, ExecutionState
from state.client import Client
from state.data import PipelineExecution

from datetime import datetime


class Pipeline:
    def __init__(self, name: str, tasks: List[Task], state_client: Client, logger):
        self.name = name
        self.tasks = tasks
        self.client = state_client
        self.parallelism = 10
        self.retry_max = 5
        self.retry_current = 0
        self.retry_policy = "ONLY_FAILED"

    pipeline_state: ExecutionState = ExecutionState.SUBMITTED
    started: datetime = None
    ended: datetime = None
    failed_tasks: set[Task] = set()

    def pprint(self):
        print("->".join([x.name for x in self.tasks]))

    """
    This function should use the client to externalize the state to a persistent storage 
    """

    def _update_execution_state(self):
        pass

    def execute_pipeline(self):
        # Register the pipeline or retrieve the pid
        pipeline_id = self.client.register_pipeline(self.name)

        # Check if pipeline execution exists
        if not self.client.exists_pipeline_execution(pipeline_id):
            pe = PipelineExecution(
                pipeline_id=pipeline_id,
                execution_id=0,  # TODO optional
                state="RUNNING",
                started=datetime.now(),
                ended=None,  # TODO optional
                parallelism=self.parallelism,
                retry_times=self.retry_current,
                retry_policy=self.retry_policy,
            )
            self.client.register_pipeline_execution(pe)

        pipeline_execution = self.client.get_pipeline_execution(pipeline_id)
        print(f"Starting pipeline execution with {pipeline_execution.execution_id}")

        # todo register tasks and get failed tasks from state
        all_tasks = self.tasks
        failed_tasks = self._execute_tasks_concurrently(all_tasks)

        if failed_tasks:
            for retry_number in range(1, self.retry_max + 1):
                self.retry_current = retry_number
                print(f"\nRetrying {retry_number}\n")  # Retry only failed tasks
                failed_tasks = self._execute_tasks_concurrently(failed_tasks)
                if not failed_tasks:
                    break

        pipeline_execution.ended = datetime.now()
        pipeline_execution.retry_times = self.retry_current

        if not failed_tasks:
            print("Pipeline executed successfully!")
            pipeline_execution.state = ExecutionState.SUCCESS.name
            self.client.update_pipeline_execution(pipeline_execution)
            return True
        else:
            print(f"Pipeline failed: {failed_tasks}")
            pipeline_execution.state = ExecutionState.FAILURE.name
            self.client.update_pipeline_execution(pipeline_execution)
            return False

    # Should return a Future of Pipeline execution status
    # use async thread pool with concurrency here
    def _execute_tasks_concurrently(self, concurrent_tasks: List[Task]) -> List[Task]:
        failed_tasks = list()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.parallelism
        ) as executor:
            execution_futures = {
                executor.submit(task.run): task for task in concurrent_tasks
            }

        for future in concurrent.futures.as_completed(execution_futures):
            task = execution_futures[future]
            print(f"{task.name} ended with: {future.result()}")

            if not task.is_successful():
                failed_tasks.append(task)

        return failed_tasks
