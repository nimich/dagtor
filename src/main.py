from dag import Pipeline
from dag.notebook_task import NotebookTask
from state.client import Client

if __name__ == "__main__":
    execution_tasks = [
        NotebookTask("task1"),
        NotebookTask("task2"),
        NotebookTask("task3"),
    ]
    ingestion_pipeline = Pipeline(
        name="ingestion", tasks=execution_tasks, state_client=Client(), logger=""
    )

    ingestion_pipeline.pprint()
    ingestion_pipeline.execute_pipeline()

    # ingestion_pipeline.execute_serially()
