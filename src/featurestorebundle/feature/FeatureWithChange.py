from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FeatureWithChange:
    name_template: str
    description_template: str
    default_value: Any
