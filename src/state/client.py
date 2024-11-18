import psycopg
from psycopg.rows import class_row
from .data import Pipeline, PipelineExecution


# TODO make this a protocol
# register
# get/update pipeline execution
# get/update tasks execution
class Client:
    def __init__(self):
        self.client_context = (
            "host=localhost dbname=dagtor user=postgres port=5432 password=example"
        )

    def __create_schema(self):
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
            """
            CREATE TABLE pipeline_execution (
                execution_id SERIAL PRIMARY KEY,
                state TEXT NOT NULL,
                started TIMESTAMP NOT NULL,
                ended TIMESTAMP NOT NULL,
                parallelism INT NOT NULL,
                retry_times INT NOT NULL,
                retry_policy TEXT
            )
            """,
        ]

        with psycopg.connect(self.client_context) as conn:
            for q in schema_creation_queries:
                conn.execute(q)
            conn.commit()

    def register_pipeline(self, pipeline_name: str) -> int:
        self.__create_schema()
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

    def register_pipeline_execution(self, execution: PipelineExecution) -> int:
        # check if the execution exists already
        return 1

    def __update_pipeline_execution(self, execution: PipelineExecution):
        query = """
          UPDATE pipeline_execution
          SET
              state = %(state)s,
              started = %(started)s,
              ended = %(ended)s,
              parallelism = %(parallelism)s,
              retry_times = %(retry_times)s,
              retry_policy = %(retry_policy)s
          WHERE execution_id = %(execution_id)s;
          """
        with psycopg.connect(self.client_context) as conn:
            conn.execute(query, execution.__dict__)
            conn.commit()
