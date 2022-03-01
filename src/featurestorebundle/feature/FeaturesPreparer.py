from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame

from featurestorebundle.delta.feature.DeltaRainbowTableManager import DeltaRainbowTableManager
from featurestorebundle.delta.feature.FeaturesJoiner import FeaturesJoiner
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage


@dataclass(frozen=True)
class WriteConfig:
    features_data: DataFrame
    rainbow_data: DataFrame
    pk_columns: List[str]


class FeaturesPreparer:
    def __init__(self, rainbow_table_manager: DeltaRainbowTableManager, features_joiner: FeaturesJoiner):
        self.__rainbow_table_manager = rainbow_table_manager
        self.__features_joiner = features_joiner

    def prepare(self, entity: Entity, feature_store_df: DataFrame, features_storage: FeaturesStorage, pk_columns: List[str]):
        rainbow_table = self.__rainbow_table_manager.read_safe(entity.name)
        features_data, rainbow_data = self.__features_joiner.join(features_storage, feature_store_df, rainbow_table)
        return WriteConfig(features_data, rainbow_data, pk_columns)
