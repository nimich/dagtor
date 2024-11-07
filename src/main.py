from dag import Pipeline
from dag.notebook_task import NotebookTask

if __name__ == "__main__":
    execution_tasks = [
        NotebookTask("task1"),
        NotebookTask("task2"),
        NotebookTask("task3"),
    ]
    ingestion_pipeline = Pipeline("ingestion", execution_tasks)

    ingestion_pipeline.register()
    ingestion_pipeline.pprint()
    ingestion_pipeline.execute()

    # ingestion_pipeline.execute_serially()
