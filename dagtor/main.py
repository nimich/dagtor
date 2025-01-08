from dag import Pipeline, MockTask


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

    state_client_config = {
        "host": "localhost",
        "dbname": "dagtor",
        "user": "postgres",
        "port": 5432,
        "password": "example",
    }

    mock_pipeline = Pipeline(
        name="mock-pipeline",
        tasks=mock_execution_tasks,
        retry_max=5,
        state_client_config=state_client_config,
    )

    mock_pipeline.execute_pipeline()
