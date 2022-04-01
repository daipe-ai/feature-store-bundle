from dataclasses import dataclass
from typing import Optional, Any


@dataclass(frozen=True)
class FeatureTemplate:
    name_template: str
    description_template: str
    fillna_value: Any
    fillna_value_type: str
    category: Optional[str] = None
