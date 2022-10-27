from pyspark.sql import SparkSession
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.table.TableCreator import TableCreator
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.writer.FeaturesTableRegistrator import FeaturesTableRegistrator
from featurestorebundle.feature.schema import get_feature_store_initial_schema


class FeaturesTablePreparer:
    def __init__(
        self,
        spark: SparkSession,
        table_creator: TableCreator,
        features_registrator: FeaturesTableRegistrator,
    ):
        self.__spark = spark
        self.__table_creator = table_creator
        self.__features_registrator = features_registrator

    def prepare(self, full_table_name: str, path: str, entity: Entity, current_feature_list: FeatureList):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_table_name.split('.')[0]}")
        self.__table_creator.create_if_not_exists(full_table_name, path, get_feature_store_initial_schema(entity), entity.time_column)
        self.__features_registrator.register(full_table_name, current_feature_list)
