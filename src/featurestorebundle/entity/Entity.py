from dataclasses import dataclass
from typing import List


@dataclass(repr=True, frozen=True)
class Entity:
    name: str
    id_column: str
    id_column_type: str
    time_column: str
    time_column_type: str

    def get_primary_key(self) -> List[str]:
        return [self.id_column, self.time_column]
