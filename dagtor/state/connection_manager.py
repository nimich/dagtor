import psycopg
from psycopg.rows import namedtuple_row

class PostgressManager:
    def __init__(self, connection_config):
        self.connection_config = connection_config

    def fetch_data(self, query, params=None):
        with psycopg.connect(**self.connection_config) as conn:
            cursor = conn.cursor(row_factory=namedtuple_row)
            return cursor.execute(query, params)

    def commit_data(self, query, params=None):
        with psycopg.connect(**self.connection_config) as conn:
            conn.execute(query, params)
            conn.commit()

    #TODO split this
    def create_schema(self):
        schema_creation_queries = [
            """CREATE SCHEMA IF NOT EXISTS state""",
            """CREATE TABLE IF NOT EXISTS state.pipeline (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS state.pipeline_execution (
                execution_id SERIAL PRIMARY KEY, 
                pipeline_id INTEGER REFERENCES state.pipeline(id),
                state TEXT NOT NULL,
                started TIMESTAMP NOT NULL,
                ended TIMESTAMP NULL,
                parallelism INT NOT NULL,
                retry_times INT NOT NULL,
                retry_policy TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS state.task_execution(
                pipeline_id INTEGER REFERENCES state.pipeline(id),
                pipeline_execution_id INTEGER REFERENCES state.pipeline_execution(execution_id),
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                started TIMESTAMP NOT NULL,
                ended TIMESTAMP NULL
            )"""
        ]
        self.execute_queries(schema_creation_queries)