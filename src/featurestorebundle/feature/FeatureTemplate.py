from dataclasses import dataclass
from typing import List, Any
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
    category: str
    owner: str
    tags: List[str]
    start_date: datetime
    frequency: str
    last_compute_date: datetime
