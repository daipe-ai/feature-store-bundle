from logging import Logger
from pyspark.sql import SparkSession
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.databricks.feature.writer.DatabricksFeatureStoreMergeConfig import DatabricksFeatureStoreMergeConfig


class DatabricksFeatureStoreDataHandler:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_existence_checker: TableExistenceChecker,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_existence_checker = table_existence_checker

    def merge_to_databricks_feature_store(self, full_table_name: str, config: DatabricksFeatureStoreMergeConfig):
        try:
            from databricks import feature_store  # pyre-ignore[21] pylint: disable=import-outside-toplevel

        except ImportError as exception:
            raise Exception("Cannot import Databricks Feature Store, you need to use ML cluster with DBR 8.3+") from exception

        dbx_feature_store_client = feature_store.FeatureStoreClient()
        db_name = full_table_name.split(".")[0]

        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        if not self.__table_exists(dbx_feature_store_client, full_table_name):
            dbx_feature_store_client.create_feature_table(
                name=full_table_name, keys=config.pk_columns, schema=config.data.select(config.pk_columns).schema
            )

        self.__logger.info(f"Writing feature data into Databricks Feature Store table {full_table_name}")

        dbx_feature_store_client.write_table(
            name=full_table_name,
            df=config.data,
            mode="merge",
        )

    def __table_exists(self, fs_client, full_table_name: str) -> bool:
        if self.__table_exists_in_feature_store(fs_client, full_table_name) and not self.__table_exists_in_hive(full_table_name):
            raise Exception(f"Table {full_table_name} exists in Databricks Feature Store but not in hive")

        if not self.__table_exists_in_feature_store(fs_client, full_table_name) and self.__table_exists_in_hive(full_table_name):
            raise Exception(f"Table {full_table_name} exists in hive but not in Databricks Feature Store")

        if self.__table_exists_in_feature_store(fs_client, full_table_name) and self.__table_exists_in_hive(full_table_name):
            self.__logger.info(f"Databricks Feature Store table {full_table_name} already exists, creation skipped")
            return True

        if not self.__table_exists_in_feature_store(fs_client, full_table_name) and not self.__table_exists_in_hive(full_table_name):
            self.__logger.info(f"Creating new Databricks Feature Store table {full_table_name}")
            return False

        raise Exception(f"Databricks Feature store table {full_table_name} is in inconsistent state")

    def __table_exists_in_feature_store(self, dbx_feature_store_client, full_table_name: str) -> bool:  # noqa
        try:
            dbx_feature_store_client.get_feature_table(full_table_name)
            return True

        except Exception:  # noqa pylint: disable=broad-except
            return False

    def __table_exists_in_hive(self, full_table_name: str) -> bool:
        return self.__table_existence_checker.exists(full_table_name)
