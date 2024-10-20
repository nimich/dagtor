from backend import Client
from .task import Task
from typing import List


class Pipeline:
    def __init__(self, name: str, tasks: List[Task]):
        self.name = name
        self.tasks = tasks
        self.client = Client()

    def pprint(self):
        print("->".join([x.name for x in self.tasks]))

    def register(self):
        self.client.register_pipeline(self.name)
        return self

    # Should return a Future of Pipeline exeution status
    # use async thread pool with concurency here
    def execute():
        return 0
