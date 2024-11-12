from typing import List
import concurrent.futures

from .task import Task, ExecutionState
from state.client import Client


class Pipeline:
    def __init__(self, name: str, tasks: List[Task], state_client: Client, looger):
        self.name = name
        self.tasks = tasks
        self.client = state_client
        self.parallelism = 10
        self.retry_times = 5
        self.retry_policy = "ONLY_FAILED"

    pipeline_state = ExecutionState.SUBMITTED
    failed_tasks = set()

    def pprint(self):
        print("->".join([x.name for x in self.tasks]))

    def register(self) -> int:
        return self.client.register_pipeline(self.name)

    """
    This function should use the client to externalize the state to a persistent storage 
    """

    def _update_execution_state(self):
        pass

    def execute_pipeline(self):
        all_tasks = self.tasks
        failed_tasks = self._execute_tasks_concurrently(all_tasks)

        if failed_tasks:
            for retry in range(1, self.retry_times + 1):
                print(f"\nRetrying {retry}\n")  # Retry only failed tasks
                failed_tasks = self._execute_tasks_concurrently(failed_tasks)
                if not failed_tasks:
                    break

        # Update the failed_tasks attribute and decide next steps
        if not failed_tasks:
            print("Pipeline executed successfully!")
            return True
        else:
            print(f"Pipeline failed: {failed_tasks}")
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

    def execute_serially(self):
        for task in self.tasks:
            result = task.run()
            print(f"{task.name} ended with: {result}")
