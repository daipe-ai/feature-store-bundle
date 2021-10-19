from dataclasses import dataclass
from pyspark.sql import DataFrame
from typing import List


@dataclass(frozen=True)
class DatabricksMergeConfig:
    data: DataFrame
    pk_columns: List[str]
