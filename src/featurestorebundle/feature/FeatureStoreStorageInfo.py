from dataclasses import dataclass
from pyspark.sql import DataFrame


@dataclass(frozen=True)
class FeatureStoreStorageInfo:
    feature_store: DataFrame
    location: str
    backend: str
