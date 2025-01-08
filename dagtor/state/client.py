from .connection_manager import PostgressManager
from .pipeline_manager import PipelineManager
from .pipeline_execution_manager import PipelineExecutionManager
from .task_execution_manager import TaskExecutionManager

class Client:
    def __init__(self, connection_string: dict):
        self.connection_manager = PostgressManager(connection_string)

        self.pipeline_manager = PipelineManager(self.connection_manager)
        self.pipeline_manager.create_table()

        self.pipeline_execution_manager = PipelineExecutionManager(self.connection_manager)
        self.pipeline_execution_manager.create_table()

        self.task_execution_manager = TaskExecutionManager(self.connection_manager)
        self.task_execution_manager.create_pipeline_execution_table_definition()
