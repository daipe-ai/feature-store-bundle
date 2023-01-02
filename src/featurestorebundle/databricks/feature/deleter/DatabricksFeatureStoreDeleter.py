from typing import List
from logging import Logger
from pyspark.sql import SparkSession
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.metadata.deleter.MetadataDeleter import MetadataDeleter
from featurestorebundle.delta.feature.deleter.DeleteColumnsQueryBuilder import DeleteColumnsQueryBuilder
from featurestorebundle.databricks.DatabricksFeatureStoreClientFactory import DatabricksFeatureStoreClientFactory
from featurestorebundle.feature.deleter.FeaturesDeleterInterface import FeaturesDeleterInterface


class DatabricksFeatureStoreDeleter(FeaturesDeleterInterface):
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_names: TableNames,
        entity_getter: EntityGetter,
        metadata_deleter: MetadataDeleter,
        delete_columns_query_builder: DeleteColumnsQueryBuilder,
        feature_store_client_factory: DatabricksFeatureStoreClientFactory,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__entity_getter = entity_getter
        self.__metadata_deleter = metadata_deleter
        self.__delete_columns_query_builder = delete_columns_query_builder
        self.__feature_store_client = feature_store_client_factory.create()

    def delete(self, features: List[str]):
        entity = self.__entity_getter.get()
        full_table_name = self.__table_names.get_features_full_table_name(entity.name)
        path = self.__table_names.get_features_path(entity.name)
        delete_query = self.__delete_columns_query_builder.build_delete_columns_query(full_table_name, features)

        self.__logger.info(f"Deleting features {', '.join(features)}")

        self.__spark.sql(delete_query)

        self.__feature_store_client.drop_table(full_table_name)

        self.__spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{path}'")

        self.__feature_store_client.register_table(delta_table=full_table_name, primary_keys=entity.get_primary_key())

        self.__metadata_deleter.delete(features)

        self.__logger.info("Deleting features done")
