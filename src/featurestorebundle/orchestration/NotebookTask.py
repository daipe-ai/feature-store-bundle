from dataclasses import dataclass, field
from typing import Dict


@dataclass
class NotebookTask:
    path: str
    parameters: Dict[str, str] = field(default_factory=dict)
    timeout: int = 86400
    retry: int = 0
