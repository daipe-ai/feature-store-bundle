from typing import Optional

from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate


class FeatureWithChange(Feature):
    def create_template(self, category: Optional[str]) -> FeatureTemplate:
        return FeatureWithChangeTemplate(
            self.name_template, self.description_template, self.fillna_with, type(self.fillna_with).__name__, category
        )
