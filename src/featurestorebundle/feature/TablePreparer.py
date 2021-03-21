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

    def prepare(self, entity: Entity, current_feature_list: FeatureList):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.__table_names.get_dbname(entity.name)}")

        self.__empty_table_creator.create(entity)

        registered_feature_list = self.__features_manager.get_features(entity.name)
        unregistered_feature_list = current_feature_list.get_unregistered(registered_feature_list.get_names())

        if not unregistered_feature_list.is_empty():
            self.__features_manager.register(entity.name, unregistered_feature_list)
