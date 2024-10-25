from state import Client
from .task import Task
from typing import List
import concurrent.futures


class Pipeline:
    def __init__(self, name: str, tasks: List[Task]):
        self.name = name
        self.tasks = tasks
        self.client = Client({})
        self.parallelism = 10

    def pprint(self):
        print("->".join([x.name for x in self.tasks]))

    def register(self) -> int:
        return self.client.register_pipeline(self.name)

    # Should return a Future of Pipeline exeution status
    # use async thread pool with concurency here
    def execute(self):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.parallelism
        ) as executor:
            execution_futures = {executor.submit(task.run): task for task in self.tasks}

        for future in concurrent.futures.as_completed(execution_futures):
            task = execution_futures[future]
            try:
                print(f"{task.name} ended with: {future.result()}")
                task.isSuccessfull()
            except Exception as e:
                print(f"Task {task.name} failed with exception: {e}")

    def execute_serially(self):
        for task in self.tasks:
            task.run()
            print(f"{task.name} ended with: {task.execution_status}")
