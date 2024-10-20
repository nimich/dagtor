class Task:
    def __init__(
        self,
        name: str,
        id: int = 0,
        execution_path: str = "",
        execution_config: str = "",
        execution_status="SUMBITTED",
    ):
        self.name = name
        self.id = id
        self.execution_path = execution_path
        self.execution_config = execution_config
        self.execution_status = execution_status  # TODO ENUM

    def run(self):
        print(f"Executing task {self.name}")
        self.execution_status = "SUCCEDED"
        return self.execution_status

    def isSuccessfull(self) -> bool:
        return self.execution_status == "SUCCEDED"
