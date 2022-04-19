from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


@dataclass(frozen=True)
class Feature:
    name_template: str
    description_template: str
    fillna_with: Any

    def create_template(
        self,
        category: Optional[str],
        owner: Optional[str],
        start_date: Optional[datetime],
        frequency: Optional[str],
        last_compute_date: Optional[datetime],
    ) -> FeatureTemplate:
        return FeatureTemplate(
            self.name_template,
            self.description_template,
            self.fillna_with,
            type(self.fillna_with).__name__,
            category,
            owner,
            start_date,
            frequency,
            last_compute_date,
        )
