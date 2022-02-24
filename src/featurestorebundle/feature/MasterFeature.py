from dataclasses import dataclass
from typing import List

from featurestorebundle.feature.FeatureInstance import FeatureInstance


@dataclass(frozen=True, eq=True)
class MasterFeature:
    name: str
    features: List[FeatureInstance]
    time_windows: List[str]
