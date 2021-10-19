from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from typing import Dict


@dataclass(frozen=True)
class DeltaMergeConfig:
    source: str
    target: str

    data: DataFrame

    merge_condition: str
    update_set: Dict[str, str] = field(default_factory=dict)
    insert_set: Dict[str, str] = field(default_factory=dict)
