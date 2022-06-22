from dataclasses import dataclass
from typing import Optional, Any
from datetime import datetime


# pylint: disable=too-many-instance-attributes
@dataclass(frozen=True)
class FeatureTemplate:
    name_template: str
    description_template: str
    fillna_value: Any
    fillna_value_type: str
    location: str
    backend: str
    notebook: str
    category: Optional[str] = None
    owner: Optional[str] = None
    start_date: Optional[datetime] = None
    frequency: Optional[str] = None
    last_compute_date: Optional[datetime] = None
    is_feature: bool = True
