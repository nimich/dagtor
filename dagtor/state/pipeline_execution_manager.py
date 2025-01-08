from typing import Optional

from .data import PipelineExecution
from .connection_manager import PostgressManager


class PipelineExecutionManager:
    def __init__(self, cm: PostgressManager):
        self.__cm = cm
        self.pipeline_execution_table_definition = """
        CREATE TABLE IF NOT EXISTS state.pipeline_execution (
            execution_id SERIAL PRIMARY KEY, 
            pipeline_id INTEGER REFERENCES state.pipeline(id),
            state TEXT NOT NULL,
            started TIMESTAMP NOT NULL,
            ended TIMESTAMP NULL,
            parallelism INT NOT NULL,
            retry_times INT NOT NULL,
            retry_policy TEXT NOT NULL
        )"""

    def create_table(self):
        self.__cm.commit_data(self.pipeline_execution_table_definition)

    def get_running_pipeline_execution(
            self,
            pipeline_id: int) -> Optional[PipelineExecution]:
        query = f"""
            SELECT * 
                FROM state.pipeline_execution 
                WHERE pipeline_id = {pipeline_id} AND state = 'RUNNING'
        """
        return self.__cm.fetch_data(query).fetchone()

    def create_pipeline_execution(self, pipeline_id, state, started, ended, parallelism, retry_times, retry_policy):
        query = """
            INSERT INTO state.pipeline_execution
            (pipeline_id, state, started, ended, parallelism, retry_times, retry_policy)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (pipeline_id, state, started, ended, parallelism, retry_times, retry_policy)
        self.__cm.commit_data(query, params)

    def update_pipeline_execution(self, pe: PipelineExecution):
        query = f"""
            UPDATE state.pipeline_execution
            SET state = '{pe.state}', started = '{pe.started}', ended = '{pe.ended}',
                parallelism = {pe.parallelism}, retry_times = {pe.retry_times}, 
                retry_policy = '{pe.retry_policy}'
            WHERE execution_id = {pe.execution_id}
        """
        self.__cm.commit_data(query)
