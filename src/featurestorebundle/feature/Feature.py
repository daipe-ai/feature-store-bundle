from dataclasses import dataclass
from typing import List, Any
from datetime import datetime

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


@dataclass(frozen=True)
class Feature:
    name_template: str
    description_template: str
    fillna_with: Any

    def create_template(
        self,
        location: str,
        backend: str,
        notebook: str,
        category: str,
        owner: str,
        tags: List[str],
        start_date: datetime,
        frequency: str,
        last_compute_date: datetime,
    ) -> FeatureTemplate:
        return FeatureTemplate(
            name_template=self.name_template,
            description_template=self.description_template,
            fillna_value=self.fillna_with,
            fillna_value_type=type(self.fillna_with).__name__,
            location=location,
            backend=backend,
            notebook=notebook,
            category=category,
            owner=owner,
            tags=tags,
            start_date=start_date,
            frequency=frequency,
            last_compute_date=last_compute_date,
        )
