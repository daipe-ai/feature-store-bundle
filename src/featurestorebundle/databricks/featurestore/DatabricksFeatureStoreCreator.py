from typing import List, Union, Optional
from logging import Logger
from pyspark.sql.types import StructType
from featurestorebundle.databricks.featurestore.DatabricksFeatureStoreClientFactory import DatabricksFeatureStoreClientFactory
from featurestorebundle.databricks.featurestore.DatabricksFeatureStoreExistenceChecker import DatabricksFeatureStoreExistenceChecker


class DatabricksFeatureStoreCreator:
    def __init__(
        self,
        logger: Logger,
        feature_store_client_factory: DatabricksFeatureStoreClientFactory,
        feature_store_existence_checker: DatabricksFeatureStoreExistenceChecker,
    ):
        self.__logger = logger
        self.__feature_store_existence_checker = feature_store_existence_checker
        self.__feature_store_client = feature_store_client_factory.create()

    def create_if_not_exists(
        self, full_table_name: str, path: str, schema: StructType, partition_by: Optional[Union[str, List[str]]] = None
    ):
        partition_by = partition_by or []
        partition_by = [partition_by] if isinstance(partition_by, str) else partition_by

        if self.__feature_store_existence_checker.exists(full_table_name):
            self.__logger.info(f"Databricks Feature Store table {full_table_name} already exists, creation skipped")
            return

        self.__logger.info(f"Creating new Databricks Feature Store table {full_table_name}")

        self.__feature_store_client.create_table(
            name=full_table_name,
            path=path,
            schema=schema,
            primary_keys=schema.fieldNames(),
            partition_columns=partition_by,
        )

        self.__logger.info(f"Databricks Feature Store table {full_table_name} successfully created")
