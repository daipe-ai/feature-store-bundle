from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame

from featurestorebundle.databricks.feature.writer.DatabricksFeatureStoreMergeConfig import DatabricksFeatureStoreMergeConfig
from featurestorebundle.delta.feature.DeltaRainbowTableManager import DeltaRainbowTableManager
from featurestorebundle.delta.feature.FeaturesJoiner import FeaturesJoiner
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


@dataclass(frozen=True)
class WriteConfig:
    features_data: DataFrame
    rainbow_data: DataFrame
    databricks_merge_config: DatabricksFeatureStoreMergeConfig


class FeaturesPreparer:
    def __init__(
        self, features_reader: FeaturesReaderInterface, rainbow_table_manager: DeltaRainbowTableManager, features_joiner: FeaturesJoiner
    ):
        self.__features_reader = features_reader
        self.__rainbow_table_manager = rainbow_table_manager
        self.__features_joiner = features_joiner

    def prepare(self, entity: Entity, features_storage: FeaturesStorage, pk_columns: List[str]):
        feature_store = self.__features_reader.read_safe(entity.name)
        rainbow_table = self.__rainbow_table_manager.read_safe(entity.name)
        features_data, rainbow_data = self.__features_joiner.join(features_storage, feature_store, rainbow_table)
        databricks_merge_config = DatabricksFeatureStoreMergeConfig(features_data, pk_columns)
        return WriteConfig(features_data, rainbow_data, databricks_merge_config)
