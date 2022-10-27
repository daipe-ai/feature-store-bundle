from pyspark.sql import SparkSession
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.schema import get_feature_store_initial_schema
from featurestorebundle.databricks.featurestore.DatabricksFeatureStoreCreator import DatabricksFeatureStoreCreator


class DatabricksFeatureStorePreparer:
    def __init__(
        self,
        spark: SparkSession,
        feature_store_creator: DatabricksFeatureStoreCreator,
    ):
        self.__spark = spark
        self.__feature_store_creator = feature_store_creator

    def prepare(self, full_table_name: str, path: str, entity: Entity):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_table_name.split('.')[0]}")

        self.__feature_store_creator.create_if_not_exists(
            full_table_name=full_table_name,
            path=path,
            schema=get_feature_store_initial_schema(entity),
            partition_by=entity.time_column,
        )
