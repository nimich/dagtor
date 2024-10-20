from dag import Pipeline, Task

if __name__ == "__main__":
    execution_tasks = [Task("task"), Task("task2"), Task("task3")]
    ingestion_pipeline = Pipeline("ingestion", execution_tasks)

    p = ingestion_pipeline.register()
    p.pprint()
