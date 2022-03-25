from dataclasses import dataclass
from typing import List
from pyspark.sql import types as t


@dataclass(repr=True, frozen=True)
class Entity:
    name: str
    id_column: str
    id_column_type: t.DataType
    time_column: str
    time_column_type: t.DataType

    def get_primary_key(self) -> List[str]:
        return [self.id_column, self.time_column]
