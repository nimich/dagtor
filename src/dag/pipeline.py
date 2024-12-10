from typing import List
import concurrent.futures

from .task import Task
from .execution_state import ExecutionState
from src.state.client import Client
from src.state.data import PipelineExecution

from datetime import datetime
from src.logger import logger


class Pipeline:
    def __init__(
        self,
        name: str,
        tasks: List[Task],
        state_client: Client,
        parallelism: int = 10,
        retry_max: int = 5,
        retry_policy: str = "ONLY_FAILED",  # TODO ENUM
    ):
        self.name = name
        self.tasks = tasks
        self.client = (
            state_client  # TODO check if this should be class attribute like a sigleton
        )
        self.parallelism = parallelism
        self.retry_max = retry_max
        self.retry_policy = retry_policy

        self.pipeline_id = None
        self.execution_id = None
        self.retry_current = 0  # Current retry Number
        self.state: ExecutionState = ExecutionState.SUBMITTED
        self.started: datetime = None
        self.ended: datetime = None

        self.execution_failed_tasks: set[Task] = set()
        self.execution_running_tasks: set[Task] = set()

        self.create_execution_dependencies()
        self.validate_acyclic_graph()
        self.validate_task_name_uniqueness()

    def validate_task_name_uniqueness(self):
        discovered_names = set()
        for task in self.tasks:
            if task.name in discovered_names:
                raise Exception(f"Not unique task name: {task.name}")
            discovered_names.add(task.name)

    def validate_acyclic_graph(self):
        def dfs(node, visited, recursion_stack):
            if node in recursion_stack:
                raise Exception(f"Cycle detected with node: {node.name}")

            if node in visited:
                return

            visited.add(node)
            recursion_stack.add(node)

            for triggered_task in node.triggers:
                dfs(triggered_task, visited, recursion_stack)

            recursion_stack.remove(node)

        visited = set()
        recursion_stack = set()

        for task in self.tasks:
            if task not in visited:
                dfs(task, visited, recursion_stack)

    def to_dataclass(self) -> PipelineExecution:
        return PipelineExecution(
            pipeline_id=self.pipeline_id,
            execution_id=self.execution_id,
            state=str(self.state),
            started=self.started,
            ended=self.ended,
            parallelism=self.parallelism,
            retry_times=self.retry_current,
            retry_policy=self.retry_policy,
        )

    def from_dataclass(self, pe: PipelineExecution):
        self.pipeline_id = pe.pipeline_id
        self.execution_id = pe.execution_id
        self.state = ExecutionState[pe.state]
        self.started = pe.started
        self.ended = pe.ended
        self.parallelism = pe.parallelism
        self.retry_current = pe.retry_times
        self.retry_policy = pe.retry_policy

    def get_or_create_pipeline_execution(self, client: Client) -> PipelineExecution:
        """
        Create and persist a pipeline execution
        If the pipeline persists in state as running which indicates a retry
        get the existing pipeline state
        :return: return success if the pipeline is registered
        """
        self.pipeline_id = client.get_or_create_pipeline(self.name)
        logger.debug(f"Pipeline id is {self.pipeline_id}")

        pe = client.get_running_pipeline_execution(self.pipeline_id)

        if pe is None:
            client.create_pipeline_execution(
                pipeline_id=self.pipeline_id,
                state=ExecutionState.RUNNING.to_string(),
                started=datetime.now(),
                ended=self.ended,
                parallelism=self.parallelism,
                retry_times=self.retry_current,
                retry_policy=self.retry_policy,
            )

        return client.get_running_pipeline_execution(self.pipeline_id)

    def create_execution_dependencies(self):
        """

        :return:
        """
        # TODO validate error for cycles
        for task in self.tasks:
            if not task.depends_on:
                self.execution_running_tasks.add(task)
            else:
                for dependency in task.depends_on:
                    dependency.add_trigger(task)

    def get_or_create_task_execution(self, task):
        """
        Get or creates state and in place updates task
        :param task:
        :return:
        """
        task.pipeline_id = self.pipeline_id
        task.pipeline_execution_id = self.execution_id

        te = self.client.get_task_execution_at_state(
            pipeline_id=task.pipeline_id,
            pipeline_execution_id=task.pipeline_execution_id,
            task_name=task.name,
            state="RUNNING",
        )

        if te is None:
            self.client.create_task_execution(
                pipeline_id=task.pipeline_id,
                pipeline_execution_id=task.pipeline_execution_id,
                name=task.name,
                state="RUNNING",
                started=datetime.now(),
                ended=None,
            )

        te = self.client.get_task_execution_at_state(
            pipeline_id=task.pipeline_id,
            pipeline_execution_id=task.pipeline_execution_id,
            task_name=task.name,
            state="RUNNING",
        )

        task.from_dataclass(te)

    def task_update_state(self, task: Task):
        self.client.update_task_execution(task.to_dataclass())

    def execute_pipeline(self):
        # Register pipeline execution
        # Create or retrieve from state and update class fields
        pe = self.get_or_create_pipeline_execution(self.client)
        self.from_dataclass(pe)

        logger.info(f"Starting pipeline execution with {self.execution_id}")

        # get failed tasks from state
        failed_tasks = self._execute_tasks_in_parallel(self.execution_running_tasks)

        if failed_tasks:
            for retry_number in range(1, self.retry_max + 1):
                self.retry_current = retry_number
                logger.info(f"*** Retrying pipeline: Attempt {retry_number} ***")
                # Retry only failed tasks
                failed_tasks = self._execute_tasks_in_parallel(failed_tasks)
                if not failed_tasks:
                    break

        self.ended = datetime.now()

        if not failed_tasks:
            logger.info("Pipeline executed successfully!")
            self.state = ExecutionState.SUCCESS.name
            self.client.update_pipeline_execution(self.to_dataclass())
            return True
        else:
            logger.error(f"Pipeline failed: {failed_tasks}")
            self.state = ExecutionState.FAILURE.name
            self.client.update_pipeline_execution(self.to_dataclass())
            return False

    def _execute_tasks_in_parallel(self, concurrent_tasks: set[Task]) -> set[Task]:
        failed_tasks = set()

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.parallelism)

        def run_with_state(task: Task):
            # Wrap the task.run logic with additional behavior or state handling
            logger.info(f"Triggering task: {task.name}")
            self.get_or_create_task_execution(task)
            result = task.run()  # Invoke the actual run method
            logger.info(f"Task {task.name} finished with result: {result}")

        try:
            execution_futures = {
                executor.submit(run_with_state, task): task for task in concurrent_tasks
            }

            while execution_futures:
                for future in concurrent.futures.as_completed(execution_futures):
                    task = execution_futures.pop(future)
                    logger.info(f"{task.name} ended with: {task.state.name}")

                    completed_tasks = [
                        t.name
                        for t in self.execution_running_tasks
                        if t.state.name == "SUCCESS"
                    ]
                    logger.debug(
                        f"Completed tasks from execution list are: {completed_tasks}"
                    )

                    if not task.is_successful():  # todo inverse cases
                        failed_tasks.add(task)
                        task.execution_state = ExecutionState.FAILURE
                        task.ended = datetime.now()
                        self.task_update_state(task)
                    else:
                        # At successful completion successful check if we can trigger another task
                        task.execution_state = ExecutionState.SUCCESS
                        task.ended = datetime.now()
                        self.task_update_state(task)

                        triggers = task.triggers
                        for trigger in triggers:
                            if trigger not in self.execution_running_tasks:
                                if trigger.dependencies_ended():
                                    self.execution_running_tasks.add(trigger)
                                    logger.debug(
                                        f"--Trigger task: {trigger.name} from {task.name}"
                                    )
                                    # TODO check heree
                                    execution_futures[
                                        executor.submit(run_with_state, trigger)
                                    ] = trigger

                    running_tasks = [
                        t.name
                        for t in self.execution_running_tasks
                        if t.state.name == "RUNNING"
                    ]
                    logger.debug(
                        f"Running tasks from execution list are: {running_tasks}"
                    )
        except Exception as e:
            logger.error(e)
            raise e
        # finally:
        #     executor.shutdown(wait=True)
        return failed_tasks
