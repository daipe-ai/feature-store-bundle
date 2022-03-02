from dataclasses import dataclass

from pyspark.sql import DataFrame

from featurestorebundle.delta.feature.DeltaRainbowTableManager import DeltaRainbowTableManager
from featurestorebundle.delta.feature.FeaturesJoiner import FeaturesJoiner
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


@dataclass(frozen=True)
class WriteConfig:
    features_data: DataFrame
    rainbow_data: DataFrame


class FeaturesPreparer:
    def __init__(
        self, features_reader: FeaturesReaderInterface, rainbow_table_manager: DeltaRainbowTableManager, features_joiner: FeaturesJoiner
    ):
        self.__features_reader = features_reader
        self.__rainbow_table_manager = rainbow_table_manager
        self.__features_joiner = features_joiner

    def prepare(self, features_storage: FeaturesStorage) -> WriteConfig:
        feature_store_df = self.__features_reader.read_safe(features_storage.entity.name)
        rainbow_table = self.__rainbow_table_manager.read_safe(features_storage.entity.name)
        features_data, rainbow_data = self.__features_joiner.join(features_storage, feature_store_df, rainbow_table)
        return WriteConfig(features_data, rainbow_data)
