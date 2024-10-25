from dag import Pipeline, Task

if __name__ == "__main__":
    execution_tasks = [Task("task1"), Task("task2"), Task("task3")]
    ingestion_pipeline = Pipeline("ingestion", execution_tasks)

    ingestion_pipeline.register()
    ingestion_pipeline.pprint()
    ingestion_pipeline.execute()

    # print(" ---------- ")
    # ingestion_pipeline.execute_serially()
