from logging import Logger
from pyspark.sql import DataFrame
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


class DatabricksFeatureStoreReader(FeaturesReaderInterface):
    def __init__(self, logger: Logger):
        self.__logger = logger

    def read(self, location: str) -> DataFrame:
        self.__logger.info(f"Reading features from Databricks Feature Store table {location}")

        return self.__read_from_databricks_feature_store(location)

    def get_backend(self) -> str:
        return "databricks"

    def __read_from_databricks_feature_store(self, full_table_name: str) -> DataFrame:
        try:
            from databricks import feature_store  # pyre-ignore[21] pylint: disable=import-outside-toplevel

        except ImportError as exception:
            raise Exception("Cannot import Databricks Feature Store, you need to use ML cluster with DBR 8.3+") from exception

        dbx_feature_store_client = feature_store.FeatureStoreClient()

        return dbx_feature_store_client.read_table(full_table_name)
