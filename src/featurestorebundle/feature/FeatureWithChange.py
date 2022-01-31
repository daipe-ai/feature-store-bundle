from dataclasses import dataclass


@dataclass(frozen=True)
class FeatureWithChange:
    name_template: str
    description_template: str
