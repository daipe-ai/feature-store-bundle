from dataclasses import dataclass
from pyspark.sql import DataFrame
from typing import Dict


@dataclass(frozen=True)
class DeltaFeaturesMergeConfig:
    source: str
    target: str

    data: DataFrame

    merge_condition: str
    update_set: Dict[str, str]
    insert_set: Dict[str, str]
