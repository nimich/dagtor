import psycopg
from psycopg.rows import class_row, dict_row
from .data import Pipeline, PipelineExecution, TaskExecution
from typing import Optional


# TODO make this a protocol
# register
# get/update pipeline execution
# get/update tasks execution
class Client:
    def __init__(self):
        self.client_context = (
            "host=localhost dbname=dagtor user=postgres port=5432 password=example"
        )
        self._create_schema()

    def _create_schema(self):
        schema_creation_queries = [
            """
            CREATE SCHEMA IF NOT EXISTS state
            """,
            """
            CREATE TABLE IF NOT EXISTS state.pipeline (
                pipeline_id SERIAL PRIMARY KEY,
                pipeline_name VARCHAR(255) NOT NULL
            )
            """,
            # todo change execution id to id
            """
            CREATE TABLE IF NOT EXISTS state.pipeline_execution (
                execution_id SERIAL PRIMARY KEY, 
                pipeline_id integer REFERENCES state.pipeline(pipeline_id),
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
                id SERIAL PRIMARY KEY,
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

    def register_pipeline(self, pipeline_name: str) -> int:
        with psycopg.connect(self.client_context) as conn:
            row_factory = conn.cursor(row_factory=class_row(Pipeline))
            select_query = f"select pipeline_id,pipeline_name from state.pipeline WHERE pipeline_name = '{pipeline_name}'"
            print(select_query)
            row = row_factory.execute(select_query).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO state.pipeline (pipeline_name) VALUES('ingestion')"
                )
                conn.commit()
                row = row_factory.execute(select_query).fetchone()

        registered_id = row.pipeline_id  # emulate registration with id -> 0
        print(f"{pipeline_name} is registered with id {registered_id}")  # todo logger
        return registered_id

    # Use get pipeline execution?
    def exists_pipeline_execution(self, pipeline_id: int) -> bool:
        with psycopg.connect(self.client_context) as conn:
            row_factory = conn.cursor(row_factory=dict_row)
            select_query = f"""
                SELECT EXISTS (
                    SELECT 1  
                    from state.pipeline_execution
                    WHERE pipeline_id = '{pipeline_id}' AND state IN  ('RUNNING')
                    )
            """
            row = row_factory.execute(select_query).fetchone()
            return row.get("exists")

    def register_pipeline_execution(self, pe: PipelineExecution):
        with psycopg.connect(self.client_context) as conn:
            query = """
                INSERT INTO state.pipeline_execution
                (pipeline_id, state, started, ended, parallelism, retry_times, retry_policy)
                VALUES  (%s, %s, %s, %s, %s, %s, %s)
                """
            params = (
                pe.pipeline_id,
                pe.state,
                pe.started,
                pe.ended,
                pe.parallelism,
                pe.retry_times,
                pe.retry_policy,
            )
            conn.execute(query, params)
            conn.commit()

    def get_pipeline_execution(self, pipeline_id: int) -> PipelineExecution:
        with psycopg.connect(self.client_context) as conn:
            row_factory = conn.cursor(row_factory=class_row(PipelineExecution))
            select_query = f"""
                  SELECT *
                      FROM state.pipeline_execution 
                      WHERE pipeline_id = {pipeline_id} AND state in ('RUNNING')
              """  # TODO select fields
            print(select_query)
            row = row_factory.execute(select_query).fetchone()
            return row

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
          WHERE execution_id = {pe.execution_id};
          """
        with psycopg.connect(self.client_context) as conn:
            conn.execute(query)
            conn.commit()

    def exists_task_execution(
        self, pipeline_id: int, execution_id: int, task_name: str
    ) -> bool:
        with psycopg.connect(self.client_context) as conn:
            row_factory = conn.cursor(row_factory=dict_row)
            select_query = f"""
                SELECT EXISTS (
                    SELECT 1  
                    from state.task_execution
                    WHERE pipeline_id = '{pipeline_id}' 
                     AND execution_id = '{execution_id}'
                     AND task_name = '{task_name}'
                     AND state IN  ('RUNNING')
                    )
            """
            row = row_factory.execute(select_query).fetchone()
            return row.get("exists")

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

    # def exists_task_execution2(self,
    #                               pipeline_id: int,
    #                               execution_id: int,
    #                               task_name: str) -> bool:
    #     self.get_task_execution(pipeline_id,execution_id,task_name).
