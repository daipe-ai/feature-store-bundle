from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


@dataclass(frozen=True)
class Feature:
    name_template: str
    description_template: str
    fillna_with: Any
    type: Optional[str] = None

    def create_template(
        self,
        category: Optional[str],
        owner: Optional[str],
        start_date: Optional[datetime],
        frequency: Optional[str],
        last_compute_date: Optional[datetime],
    ) -> FeatureTemplate:
        return FeatureTemplate(
            name_template=self.name_template,
            description_template=self.description_template,
            fillna_value=self.fillna_with,
            fillna_value_type=type(self.fillna_with).__name__,
            type=self.type,
            category=category,
            owner=owner,
            start_date=start_date,
            frequency=frequency,
            last_compute_date=last_compute_date,
        )
