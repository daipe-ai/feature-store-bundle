from typing import Optional
from datetime import datetime

from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate


class FeatureWithChange(Feature):
    def create_template(
        self,
        category: Optional[str],
        owner: Optional[str],
        start_date: Optional[datetime],
        frequency: Optional[str],
        last_compute_date: Optional[datetime],
    ) -> FeatureTemplate:
        return FeatureWithChangeTemplate(
            self.name_template,
            self.description_template,
            self.fillna_with,
            type(self.fillna_with).__name__,
            self.type,
            category,
            owner,
            start_date,
            frequency,
            last_compute_date,
        )
