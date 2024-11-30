from dag import Pipeline
from state.data import TaskExecution
from datetime import datetime
from dag.notebook_task import NotebookTask
from state.client import Client
from state.client import exists_running_task_execution
import sys

if __name__ == "__main__":
    sys.path.insert(0, "/Users/n.michail/Projects/VsCode/dagtor/src/dag")
    sys.path.insert(0, "/Users/n.michail/Projects/VsCode/dagtor/src/state")

    state_client = Client()

    task_execution = TaskExecution(3, 90, 0, "test", "RUNNING", datetime.now(), None)
    # state_client.create_task_execution(task_execution)
    te = state_client.get_task_execution(3, 90, "test3")
    # pe = state_client.get_pipeline_execution(3)
    print(exists_running_task_execution(te))
    print(te)

    # sys.exit(0)
    task_1 = NotebookTask(name="task1")

    task_2 = NotebookTask("task2")

    task_3 = NotebookTask("task3")
    task_3.add_dependency(task_2)
    task_3.add_dependency(task_1)

    task_4 = NotebookTask("task4")
    task_4.add_dependency(task_3)

    execution_tasks = [task_1, task_2, task_3, task_4]
    ingestion_pipeline = Pipeline(
        name="ingestion", tasks=execution_tasks, state_client=Client(), logger=""
    )  # TODO validate in constructor: check for task name uniqueness and for cycles

    ingestion_pipeline.create_execution_order()
    ingestion_pipeline.print_trigger()

    ingestion_pipeline.execute_pipeline()
