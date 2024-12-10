from dag import Pipeline
from dag.mock_task import MockTask
from state.client import Client
import sys


if __name__ == "__main__":
    sys.path.insert(0, "/Users/n.michail/Projects/VsCode/dagtor/src/dag")
    sys.path.insert(0, "/Users/n.michail/Projects/VsCode/dagtor/src/state")

    task_0 = MockTask(name="task0")

    task_1 = MockTask(name="task1")
    # task_1.add_dependency(task_0)
    # task_0.add_dependency(task_1)

    task_2 = MockTask(name="task2")

    task_3 = MockTask(name="task3")
    task_3.add_dependency(task_2)
    task_3.add_dependency(task_1)

    task_4 = MockTask(name="task4")
    task_4.add_dependency(task_3)

    execution_tasks = [task_1, task_2, task_3, task_4, task_0]
    ingestion_pipeline = Pipeline(
        name="ingestion", tasks=execution_tasks, state_client=Client()
    )  # TODO validate in constructor: check for task name uniqueness and for cycles

    ingestion_pipeline.execute_pipeline()
