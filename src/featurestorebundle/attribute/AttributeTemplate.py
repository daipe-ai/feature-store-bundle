from dataclasses import dataclass

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


@dataclass(frozen=True)
class AttributeTemplate(FeatureTemplate):
    is_feature: bool = False
