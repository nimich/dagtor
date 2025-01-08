from datetime import datetime
from unittest.mock import Mock, MagicMock

import pytest

from dagtor.state.pipeline_manager import PipelineManager
from dagtor.state.connection_manager import PostgressManager


class TestManager:

    @pytest.fixture()
    def connection_manager(self):
        return MagicMock(PostgressManager)

    def test_get_existing_pipeline(self, connection_manager):
        mock_cm = connection_manager()
        test_name = "existing-pipeline"
        test_id = 1

        mock_cm.fetch_data.return_value.fetchone.return_value = (test_id, test_name)
        pip_mng = PipelineManager(mock_cm)

        pipeline_id = pip_mng.get_or_create_pipeline(test_name)

        # Verify the select query was executed
        expected_query_select = f"""
            SELECT id, name 
                FROM state.pipeline 
                WHERE name = '{test_name}'
            """.strip()

        mock_cm.fetch_data.assert_called_once_with(expected_query_select)

        # Assert the return value is as expected
        assert(pipeline_id == test_id)

    def test_create_new_pipeline(self):
        mock_cm = MagicMock(PostgressManager)
        name = "test"

        mock_fetch_data_result = MagicMock()
        mock_fetch_data_result.fetchone.side_effect = [None, (2, "test")]
        mock_cm.fetch_data.return_value = mock_fetch_data_result

        pip_mng = PipelineManager(mock_cm)

        pipeline_id = pip_mng.get_or_create_pipeline(name)

        # Verify the select query was executed
        expected_query_select = f"""INSERT INTO state.pipeline (name) VALUES('{name}')""".strip()

        mock_cm.commit_data.assert_called_once_with(expected_query_select)

        # Assert the return value is as expected
        assert(pipeline_id == 2)
