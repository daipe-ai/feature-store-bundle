from pyspark.sql import SparkSession
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.delta.TableCreator import TableCreator
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.writer.DeltaFeaturesRegistrator import DeltaFeaturesRegistrator
from featurestorebundle.delta.feature.schema import get_feature_store_initial_schema


class DeltaTableFeaturesPreparer:
    def __init__(
        self,
        spark: SparkSession,
        table_names: TableNames,
        table_creator: TableCreator,
        features_registrator: DeltaFeaturesRegistrator,
    ):
        self.__spark = spark
        self.__table_names = table_names
        self.__table_creator = table_creator
        self.__features_registrator = features_registrator

    def prepare(self, full_table_name: str, path: str, entity: Entity, current_feature_list: FeatureList):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.__table_names.get_features_database(entity.name)}")
        self.__table_creator.create_if_not_exists(full_table_name, path, get_feature_store_initial_schema(entity), entity.time_column)
        self.__features_registrator.register(full_table_name, current_feature_list)
