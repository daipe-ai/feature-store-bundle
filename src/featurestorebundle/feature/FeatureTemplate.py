from dataclasses import dataclass
from typing import Optional, Any


# pylint: disable=too-many-instance-attributes
@dataclass(frozen=True)
class FeatureTemplate:
    name_template: str
    description_template: str
    fillna_value: Any
    fillna_value_type: str
    category: Optional[str] = None
    owner: Optional[str] = None
    start_date: Optional[str] = None
    frequency: Optional[str] = None
