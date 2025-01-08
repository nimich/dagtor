from typing import Optional

from .data import TaskExecution
from .connection_manager import PostgressManager


class TaskExecutionManager:
    def __init__(self, cm: PostgressManager):
        self.__cm = cm
        self.task_execution_table_definition = """
        CREATE TABLE IF NOT EXISTS state.task_execution(
            pipeline_id integer REFERENCES state.pipeline(id),
            pipeline_execution_id integer REFERENCES state.pipeline_execution(execution_id),
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT NOT NULL,
            started TIMESTAMP NOT NULL,
            ended TIMESTAMP NULL
        )
        """

    def create_pipeline_execution_table_definition(self):
        """

        :return:
        """
        self.__cm.commit_data(self.task_execution_table_definition)

    def create_task_execution(self, pipeline_id, pipeline_execution_id, name, state, started, ended):
        """

        :param pipeline_id:
        :param pipeline_execution_id:
        :param name:
        :param state:
        :param started:
        :param ended:
        :return:
        """
        query = """
            INSERT INTO state.task_execution
            (pipeline_id, pipeline_execution_id, name, state, started, ended)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (pipeline_id, pipeline_execution_id, name, state, started, ended)
        self.__cm.commit_data(query, params)

    def update_task_execution(self, te: TaskExecution):
        """

        :param te:
        :return:
        """
        query = f"""
            UPDATE state.task_execution
            SET pipeline_id = '{te.pipeline_id}', pipeline_execution_id = '{te.pipeline_execution_id}',
                name = '{te.name}', state = '{te.state}', started = '{te.started}', ended = '{te.ended}'
            WHERE id = {te.id}
        """
        self.__cm.commit_data(query)

    def get_task_execution_at_state(self, pipeline_id, pipeline_execution_id, task_name, state="RUNNING") -> Optional[
        TaskExecution]:
        """

        :param pipeline_id:
        :param pipeline_execution_id:
        :param task_name:
        :param state:
        :return:
        """
        query = f"""
            SELECT * FROM state.task_execution 
            WHERE pipeline_id = '{pipeline_id}' AND pipeline_execution_id = '{pipeline_execution_id}'
              AND name = '{task_name}' AND state = '{state}'
        """
        return self.__cm.fetch_data(query).fetchone()

