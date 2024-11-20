import unittest
from unittest.mock import MagicMock, patch
from psycopg.rows import class_row
from state.client import Client


class TestRegisterPipeline(unittest.TestCase):
    @patch("your_module.psycopg.connect")
    def test_register_pipeline_creates_and_returns_id(self, mock_connect):
        # Arrange
        pipeline_name = "ingestion"
        pipeline_id = 1

        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Set up the mocked row factory to return a Pipeline instance or None
        Pipeline = MagicMock()
        row_factory = class_row(Pipeline)
        mock_cursor.row_factory = row_factory

        # Simulate fetchone behavior: return None for the first query, then a row
        mock_cursor.execute.side_effect = [
            mock_cursor,  # First fetch (no row found)
            mock_cursor,  # Second fetch (row created)
        ]
        mock_cursor.fetchone.side_effect = [
            None,
            MagicMock(pipeline_id=pipeline_id, pipeline_name=pipeline_name),
        ]

        # Act
        my_class_instance = Client(
            client_context="mock_context"
        )  # Replace YourClass with your actual class
        result = my_class_instance.register_pipeline(pipeline_name)

        # Assert
        self.assertEqual(result, pipeline_id)
        mock_connect.assert_called_once_with("mock_context")
        mock_cursor.execute.assert_any_call(
            f"select pipeline_id,pipeline_name from state.pipeline WHERE pipeline_name = '{pipeline_name}'"
        )
        mock_connection.execute.assert_called_once_with(
            f"INSERT INTO state.pipeline (pipeline_name) VALUES('{pipeline_name}')"
        )
        mock_connection.commit.assert_called_once()
