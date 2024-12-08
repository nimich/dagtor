from datetime import datetime
from unittest.mock import Mock

import pytest

from src.dag.pipeline import Pipeline
from src.state.data import PipelineExecution


@pytest.fixture()
def pipeline_execution():
    return PipelineExecution(
        pipeline_id=1,
        execution_id=1,
        state="RUNNING",
        started=datetime(2024, 1, 1),
        ended=datetime(2025, 1, 1),
        parallelism=10,
        retry_times=1,
        retry_policy="F",
    )


class TestPipeline:
    def test_from_dataclass(self, pipeline_execution):
        p = Pipeline(name="test", tasks=list(), state_client=Mock())
        p.from_dataclass(pe=pipeline_execution)
        assert p.pipeline_id == 1
