from dataclasses import dataclass
from typing import List
from pyspark.sql.types import DataType


@dataclass(repr=True, frozen=True)
class Entity:
    name: str
    id_column: str
    id_column_type: DataType
    time_column: str
    time_column_type: DataType

    def get_primary_key(self) -> List[str]:
        return [self.id_column, self.time_column]
