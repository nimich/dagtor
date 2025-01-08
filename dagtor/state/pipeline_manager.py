from dagtor.logger import logger
from .connection_manager import PostgressManager
from .data.pipeline import Pipeline


class PipelineManager:
    """

    """

    def __init__(self, cm: PostgressManager):
        self.__cm = cm

        self.__table_definition = """
        CREATE TABLE IF NOT EXISTS state.pipeline (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )"""

    def create_table(self):
        """

        :return:
        """
        self.__cm.commit_data(self.__table_definition)

    def get_or_create_pipeline(self, name: str) -> int:
        """

        :param name:
        :return:
        """
        query = f"""
            SELECT id, name 
                FROM state.pipeline 
                WHERE name = '{name}'
            """.strip()
        row = self.__cm.fetch_data(query).fetchone()
        if not row:
            self.__cm.commit_data(f"INSERT INTO state.pipeline (name) VALUES('{name}')")
            row = self.__cm.fetch_data(query).fetchone()

        data = Pipeline(*row)
        logger.info(f"{name} is registered with id {data.id}")
        return data.id

