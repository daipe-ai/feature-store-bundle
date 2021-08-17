from typing import List
from pyspark.sql import SparkSession
from featurestorebundle.feature.FeatureManager import FeatureManager
from featurestorebundle.db.TableNames import TableNames


class FeatureStore:
    def __init__(
        self,
        spark: SparkSession,
        feature_manager: FeatureManager,
        table_names: TableNames,
    ):
        self.__spark = spark
        self.__feature_manager = feature_manager
        self.__table_names = table_names

    def get_latest(self, entity_name: str, feature_names: List[str] = None):
        table_identifier = self.__table_names.get_latest_table_identifier(entity_name)

        return self.__get_features(table_identifier, feature_names)

    def get_historized(self, entity_name: str, feature_names: List[str] = None):
        table_identifier = self.__table_names.get_historized_table_identifier(entity_name)

        return self.__get_features(table_identifier, feature_names)

    def __get_features(self, table_identifier: str, feature_names: List[str]):
        registered_feature_names_list = self.__feature_manager.get_feature_names(table_identifier)

        if feature_names is None:
            feature_names = registered_feature_names_list

        unregistered_features = set(feature_names) - set(registered_feature_names_list)

        if unregistered_features != set():
            raise Exception(f"Features {','.join(unregistered_features)} not registered")

        return self.__feature_manager.get_values(table_identifier, feature_names)
