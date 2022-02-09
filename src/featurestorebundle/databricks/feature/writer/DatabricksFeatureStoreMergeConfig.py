from dataclasses import dataclass
from pyspark.sql import DataFrame
from typing import List


@dataclass(frozen=True)
class DatabricksFeatureStoreMergeConfig:
    data: DataFrame
    pk_columns: List[str]
