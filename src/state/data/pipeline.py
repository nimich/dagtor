from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Pipeline:
    id: int
    name: str


@dataclass
class PipelineExecution:
    execution_id: int
    pipeline_id: int
    state: str
    started: datetime
    ended: Optional[datetime]
    parallelism: int
    retry_times: int
    retry_policy: str


@dataclass
class TaskExecution:
    pipeline_id: int
    pipeline_execution_id: int
    id: int
    name: str
    state: str
    started: datetime
    ended: datetime
