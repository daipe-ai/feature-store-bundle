from dataclasses import dataclass
from typing import Any

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


@dataclass(frozen=True)
class Feature:
    name_template: str
    description_template: str
    fillna_with: Any

    def create_template(self, category: str) -> FeatureTemplate:
        return FeatureTemplate(self.name_template, self.description_template, self.fillna_with, category)
