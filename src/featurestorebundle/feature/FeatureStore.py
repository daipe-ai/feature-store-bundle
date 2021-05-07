from typing import List
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as f
from featurestorebundle.feature.FeatureManager import FeatureManager


class FeatureStore:
    def __init__(
        self,
        feature_manager: FeatureManager,
        spark: SparkSession,
    ):
        self.__feature_manager = feature_manager
        self.__spark = spark

    def get(self, entity_name: str, feature_names: List[str] = None):
        registered_feature_list = self.__feature_manager.get_features(entity_name)
        if feature_names is None:
            feature_names = registered_feature_list.get_names()
        unregistered_features = set(feature_names) - set(registered_feature_list.get_names())

        if unregistered_features != set():
            raise Exception(f"Features {','.join(unregistered_features)} not registered for entity {entity_name}")

        return self.__feature_manager.get_values(entity_name, feature_names)

    def get_latest(self, entity_name: str, feature_names: List[str] = None):
        df = self.get(entity_name, feature_names)
        id_column = df.columns[0]
        time_column = df.columns[1]
        features = df.columns[2:]

        window = (
            Window().partitionBy(id_column).orderBy(f.desc(time_column)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        return (
            df.select(id_column, *[f.first(feature, ignorenulls=True).over(window).alias(feature) for feature in features])
            .groupBy(id_column)
            .agg(*[f.first(feature).alias(feature) for feature in features])
        )
