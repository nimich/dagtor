from dag import Pipeline, MockTask, DatabrickJobTask
from state.client import Client
import sys
import os


if __name__ == "__main__":

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

    mock_execution_tasks = [task_0, task_1, task_2, task_3, task_4]


    ws_db_dev = "https://adb-5230024251530598.18.azuredatabricks.net/"
    db_token = os.environ["DATABRICKS_TOKEN"]

    state_client_config = {
        "host": "localhost",
        "dbname": "dagtor",
        "user": "postgres",
        "port": 5432,
        "password": "example"
    }

    job_task_1 = DatabrickJobTask(name="nimi-simple-job",
                                  job_id=63846187300786,
                                  host=ws_db_dev,
                                  token=db_token
                                  )
    job_task_2 = DatabrickJobTask(name="nimi-simple-job-2",
                                  job_id=1063125211419012,
                                  host=ws_db_dev,
                                  token=db_token
                                  )

    job_task_3 = DatabrickJobTask(name="nimi-simple-job-3",
                                  job_id=843564999314646,
                                  host=ws_db_dev,
                                  token=db_token
                                  )

    job_task_3.add_dependency(job_task_1)
    job_task_3.add_dependency(job_task_2)

    dbjob_execution_tasks = [job_task_1, job_task_2, job_task_3]

    ingestion_pipeline = Pipeline(
        name="databricks-ingestion",
        tasks=dbjob_execution_tasks,
        retry_max=5,
        state_client_config=state_client_config
    )  # TODO validate in constructor: check for task name uniqueness and for cycles



    mock_pipeline = Pipeline(
        name="mock-pipeline",
        tasks=mock_execution_tasks,
        retry_max=5,
        state_client_config=state_client_config
    )
    #ingestion_pipeline.execute_pipeline()
    
    mock_pipeline.execute_pipeline()
