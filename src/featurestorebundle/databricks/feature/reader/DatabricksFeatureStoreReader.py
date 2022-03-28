from logging import Logger
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


class DatabricksFeatureStoreReader(FeaturesReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        table_existence_checker: TableExistenceChecker,
    ):
        self.__logger = logger
        self.__table_names = table_names
        self.__table_existence_checker = table_existence_checker

    def read(self, entity_name: str) -> DataFrame:
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)

        self.__logger.info(f"Reading features from Databricks Feature Store table {full_table_name}")

        return self.__read_from_databricks_feature_store(full_table_name)

    def exists(self, entity_name: str) -> bool:
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)

        return self.__table_existence_checker.exists(full_table_name)

    def __read_from_databricks_feature_store(self, full_table_name: str) -> DataFrame:
        try:
            from databricks import feature_store  # pyre-ignore[21] pylint: disable=import-outside-toplevel

        except ImportError as exception:
            raise Exception("Cannot import Databricks Feature Store, you need to use ML cluster with DBR 8.3+") from exception

        dbx_feature_store_client = feature_store.FeatureStoreClient()

        return dbx_feature_store_client.read_table(full_table_name)
