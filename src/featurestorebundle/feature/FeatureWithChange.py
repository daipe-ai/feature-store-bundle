from typing import Optional
from datetime import datetime

from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate


class FeatureWithChange(Feature):
    def create_template(
        self,
        location: str,
        backend: str,
        notebook: str,
        category: Optional[str],
        owner: Optional[str],
        start_date: Optional[datetime],
        frequency: Optional[str],
        last_compute_date: Optional[datetime],
    ) -> FeatureTemplate:
        return FeatureWithChangeTemplate(
            name_template=self.name_template,
            description_template=self.description_template,
            fillna_value=self.fillna_with,
            fillna_value_type=type(self.fillna_with).__name__,
            location=location,
            backend=backend,
            notebook=notebook,
            category=category,
            owner=owner,
            start_date=start_date,
            frequency=frequency,
            last_compute_date=last_compute_date,
        )
