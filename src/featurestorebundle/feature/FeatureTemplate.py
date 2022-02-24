from dataclasses import dataclass
from typing import Optional, Any


@dataclass(frozen=True)
class FeatureTemplate:
    name_template: str
    description_template: str
    default_value: Any
    category: Optional[str] = None
