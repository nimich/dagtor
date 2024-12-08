import psycopg
from psycopg.rows import class_row
from .data import Pipeline, PipelineExecution, TaskExecution
from typing import Optional
from returns.maybe import Maybe, maybe
from src.logger import logger
from datetime import datetime


# TODO make this a protocol
# register
# get/update pipeline execution
# get/update tasks execution


def exists_running_task_execution(task: Maybe[TaskExecution]) -> bool:
    """

    :param task:
    :return:
    """
    return task.bind_optional(lambda t: t.state == "RUNNING").value_or(False)


def exists_running_pipeline_execution(pipeline: Maybe[PipelineExecution]) -> bool:
    """

    :param pipeline:
    :return:
    """
    return pipeline.value_or(False)


class Client:
    def __init__(self):
        self.client_context = (
            "host=localhost dbname=dagtor user=postgres port=5432 password=example"
        )
        self._create_schema()

    def _create_schema(self):
        """

        :return:
        """
        schema_creation_queries = [
            """
            CREATE SCHEMA IF NOT EXISTS state
            """,
            """
            CREATE TABLE IF NOT EXISTS state.pipeline (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
            """,
            # todo change execution id to id
            """
            CREATE TABLE IF NOT EXISTS state.pipeline_execution (
                execution_id SERIAL PRIMARY KEY, 
                pipeline_id integer REFERENCES state.pipeline(id),
                state TEXT NOT NULL,
                started TIMESTAMP NOT NULL,
                ended TIMESTAMP NULL,
                parallelism INT NOT NULL,
                retry_times INT NOT NULL,
                retry_policy TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS state.task_execution(
                pipeline_id integer REFERENCES state.pipeline(pipeline_id),
                pipeline_execution_id integer REFERENCES state.pipeline_execution(execution_id),
                task_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                started TIMESTAMP NOT NULL,
                ended TIMESTAMP NULL
            )
            """,
        ]

        with psycopg.connect(self.client_context) as conn:
            for q in schema_creation_queries:
                conn.execute(q)
            conn.commit()

    """
    *****************************
    ********* Pipeline  *********
    *****************************
    """

    def get_or_create_pipeline(self, name: str) -> int:
        with psycopg.connect(self.client_context) as conn:
            row_factory = conn.cursor(row_factory=class_row(Pipeline))
            select_query = f"select id, name from state.pipeline WHERE name = '{name}'"
            row = row_factory.execute(select_query).fetchone()
            if row is None:
                conn.execute("INSERT INTO state.pipeline (name) VALUES('ingestion')")
                conn.commit()
                row = row_factory.execute(select_query).fetchone()

        logger.info(f"{name} is registered with id {row.id}")
        return row.id

    """
    *****************************
    **** Pipeline execution  ****
    *****************************
    """

    def get_running_pipeline_execution(
        self, pipeline_id: int
    ) -> Optional[PipelineExecution]:
        with psycopg.connect(self.client_context) as conn:
            row_factory = conn.cursor(
                row_factory=class_row(PipelineExecution)
            )  # TODO state can be argument
            select_query = f"""
                  SELECT *
                      FROM state.pipeline_execution 
                      WHERE pipeline_id = {pipeline_id} AND state = 'RUNNING'
              """
            row = row_factory.execute(select_query).fetchone()
            return row

    def create_pipeline_execution(
        self,
        pipeline_id: int,
        state: str,
        started: datetime,
        ended: datetime,
        parallelism: int,
        retry_times: int,
        retry_policy: str,
    ):
        with psycopg.connect(self.client_context) as conn:
            query = """
                INSERT INTO state.pipeline_execution
                (pipeline_id, state, started, ended, parallelism, retry_times, retry_policy)
                VALUES  (%s, %s, %s, %s, %s, %s, %s)
                """
            params = (
                pipeline_id,
                state,
                started,
                ended,
                parallelism,
                retry_times,
                retry_policy,
            )
            conn.execute(query, params)
            conn.commit()

    def update_pipeline_execution(self, pe: PipelineExecution):
        query = f"""
          UPDATE state.pipeline_execution
          SET
              state = '{pe.state}',
              started = '{pe.started}',
              ended = '{pe.ended}',
              parallelism = {pe.parallelism},
              retry_times = {pe.retry_times},
              retry_policy = '{pe.retry_policy}'
          WHERE execution_id = {pe.execution_id} 
          """
        with psycopg.connect(self.client_context) as conn:
            conn.execute(query)
            conn.commit()

    """
    *****************************
    ****** Task execution  ******
    *****************************
    """

    def create_task_execution(self, te: TaskExecution):
        with psycopg.connect(self.client_context) as conn:
            query = """
                   INSERT INTO state.task_execution
                   (pipeline_id, pipeline_execution_id, name, state, started, ended)
                   VALUES  (%s, %s, %s, %s, %s, %s)
                   """
            params = (
                te.pipeline_id,
                te.pipeline_execution_id,
                te.name,
                te.state,
                te.started,
                te.ended,
            )
            conn.execute(query, params)
            conn.commit()

    @maybe
    def get_task_execution(
        self, pipeline_id: int, execution_id: int, task_name: str
    ) -> Optional[TaskExecution]:
        with psycopg.connect(self.client_context) as conn:
            row_factory = conn.cursor(row_factory=class_row(TaskExecution))
            select_query = f"""
                  SELECT *
                      FROM state.task_execution 
                      WHERE pipeline_id = '{pipeline_id}' 
                        AND pipeline_execution_id = '{execution_id}'
                        AND name = '{task_name}'
              """  # TODO select fields
            row = row_factory.execute(select_query).fetchone()
            return row
