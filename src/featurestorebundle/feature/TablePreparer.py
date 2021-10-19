from pyspark.sql import SparkSession
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.EmptyTableCreator import EmptyTableCreator
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureManager import FeatureManager


class TablePreparer:
    def __init__(
        self,
        table_names: TableNames,
        spark: SparkSession,
        empty_table_creator: EmptyTableCreator,
        features_manager: FeatureManager,
    ):
        self.__table_names = table_names
        self.__spark = spark
        self.__empty_table_creator = empty_table_creator
        self.__features_manager = features_manager

    def prepare(self, full_table_name: str, path: str, entity: Entity, current_feature_list: FeatureList):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.__table_names.get_db_name(entity.name)}")
        self.__empty_table_creator.create(full_table_name, path, entity)
        self.__register_features(full_table_name, current_feature_list)

    def __register_features(self, full_table_name: str, current_feature_list: FeatureList):
        registered_feature_names = self.__features_manager.get_feature_names(full_table_name)
        unregistered_features = current_feature_list.get_unregistered(registered_feature_names)

        if not unregistered_features.empty():
            self.__features_manager.register(full_table_name, unregistered_features)
