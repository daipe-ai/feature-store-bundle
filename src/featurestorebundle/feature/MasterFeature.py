from dataclasses import dataclass
from typing import List

from featurestorebundle.feature.Feature import Feature


@dataclass(frozen=True, eq=True)
class MasterFeature:
    name: str
    features: List[Feature]
    time_windows: List[str]
