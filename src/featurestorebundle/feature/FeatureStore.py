from typing import List
from pyspark.sql import SparkSession
from featurestorebundle.feature.FeatureManager import FeatureManager


class FeatureStore:
    def __init__(
        self,
        feature_manager: FeatureManager,
        spark: SparkSession,
    ):
        self.__feature_manager = feature_manager
        self.__spark = spark

    def get(self, entity_name: str, feature_names: List[str] = []):
        registered_feature_list = self.__feature_manager.get_features(entity_name)
        unregistered_features = set(feature_names) - set(registered_feature_list.get_names())

        if unregistered_features != set():
            raise Exception(f"Features {','.join(unregistered_features)} not registered for entity {entity_name}")

        if feature_names == []:
            feature_names = registered_feature_list

        return self.__feature_manager.get_values(entity_name, feature_names)
