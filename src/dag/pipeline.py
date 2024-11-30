from typing import List
import concurrent.futures

from .task import Task, ExecutionState
from src.state.client import Client
from src.state.data import PipelineExecution

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

        # TODO no side effects in constructor
        # add default NONE for all variables
        self.pipeline_id = state_client.register_pipeline(self.name)
        if not self.client.exists_pipeline_execution(self.pipeline_id):
            pe = PipelineExecution(
                pipeline_id=self.pipeline_id,
                execution_id=0,  # TODO optional
                state="RUNNING",
                started=datetime.now(),
                ended=None,  # TODO optional
                parallelism=self.parallelism,
                retry_times=self.retry_current,
                retry_policy=self.retry_policy,
            )
            self.client.register_pipeline_execution(pe)
        self.pipeline_execution = self.client.get_pipeline_execution(self.pipeline_id)

    pipeline_state: ExecutionState = ExecutionState.SUBMITTED
    # Register the pipeline or retrieve the pid
    started: datetime = None
    ended: datetime = None
    failed_tasks: set[Task] = set()

    execution_list = set()

    #     def persist_task_state(self, pipeline_id: int, execution_id: int, task_name: str):
    #         optionalte = self.client.get_task_execution(
    #             pipeline_id= pipeline_id,
    #             execution_id= execution_id,
    #             task_name= task_name
    #             )
    #         if not exists_running_task_execution(optionalte):
    # =            pipeline_execution_id: int
    #             id: int
    #             name: str
    #             state: str
    #             started: datetime
    #             ended: datetime
    #             nte = TaskExecution(
    #                 pipeline_id=self.pipeline_id,
    #                 pipeline_execution_id=
    #
    #             )
    #             self.client.create_task_execution()
    #         else:

    def print_trigger(self):
        print(f"Initial execution {[t.name for t in self.execution_list]}")
        for t in self.tasks:
            for tr in t.triggers:
                print(f"{t.name} triggers {tr.name}")

    # TODO add this in execute
    def create_execution_order(self):
        # todo reurn error for cycles
        for task in self.tasks:
            if not task.depends_on:
                self.execution_list.add(task)
            else:
                for dependency in task.depends_on:
                    dependency.add_trigger(task)

    def pprint(self):
        print("->".join([x.name for x in self.tasks]))

    """
    This function should use the client to externalize the state to a persistent storage 
    """

    def _update_execution_state(self):
        pass

    def execute_pipeline(self):
        pipeline_execution = self.pipeline_execution
        print(f"\nStarting pipeline execution with {pipeline_execution.execution_id}")

        # todo register tasks and get failed tasks from state
        all_tasks = self.execution_list
        failed_tasks = self._execute_tasks_concurrently_with_trigger(all_tasks)

        if failed_tasks:
            for retry_number in range(1, self.retry_max + 1):
                self.retry_current = retry_number
                print(f"\nRetrying {retry_number}\n")  # Retry only failed tasks
                failed_tasks = self._execute_tasks_concurrently_with_trigger(
                    failed_tasks
                )
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
    # can async thread pool for concurrency here
    def _execute_tasks_concurrently(self, concurrent_tasks: set[Task]) -> set[Task]:
        failed_tasks = set()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.parallelism
        ) as executor:
            execution_futures = {
                executor.submit(task.run): task for task in concurrent_tasks
            }

        for future in concurrent.futures.as_completed(execution_futures):
            task = execution_futures[future]
            print(f"execution list: {[t.name for t in self.execution_list]}")

            if not task.is_successful():
                failed_tasks.add(task)

        return failed_tasks

    def _execute_tasks_concurrently_with_trigger(
        self, concurrent_tasks: set[Task]
    ) -> set[Task]:
        failed_tasks = set()

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.parallelism)

        try:
            execution_futures = {
                executor.submit(task.run): task
                for task in concurrent_tasks
                # todo instead of run use run with state
            }

            while execution_futures:
                for future in concurrent.futures.as_completed(execution_futures):
                    task = execution_futures.pop(future)
                    print(f"{task.name} ended with: {future.result()}")
                    print(
                        f"execution list beofre: {[t.name for t in self.execution_list]}"
                    )

                    if not task.is_successful():  # todo inverse cases
                        failed_tasks.add(task)
                        task.execution_state = ExecutionState.FAILURE
                        # todo update state
                    else:
                        # At successful completion successful check if we can trigger
                        # another task
                        task.execution_state = ExecutionState.SUCCESS
                        # todo update state
                        triggers = task.triggers
                        for trigger in triggers:
                            if trigger not in self.execution_list:
                                print(trigger.dependencies_ended())
                                if trigger.dependencies_ended():
                                    self.execution_list.add(trigger)
                                    print(
                                        f"---->Adding trigger task: {trigger.name} from {task.name}"
                                    )
                                    execution_futures[executor.submit(trigger.run)] = (
                                        trigger
                                    )

        finally:
            executor.shutdown(wait=True)
            return failed_tasks
