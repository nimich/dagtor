from dataclasses import dataclass


@dataclass
class Pipeline:
    id: id
    name: str
    state: str
    started: str
    ended: str
