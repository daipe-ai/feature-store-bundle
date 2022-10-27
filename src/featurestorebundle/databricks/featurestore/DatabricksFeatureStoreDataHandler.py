from logging import Logger
from pyspark.sql import DataFrame
from featurestorebundle.databricks.featurestore.DatabricksFeatureStoreClientFactory import DatabricksFeatureStoreClientFactory


class DatabricksFeatureStoreDataHandler:
    def __init__(
        self,
        logger: Logger,
        feature_store_client_factory: DatabricksFeatureStoreClientFactory,
    ):
        self.__logger = logger
        self.__feature_store_client = feature_store_client_factory.create()

    def overwrite(self, full_table_name: str, df: DataFrame):
        self.__logger.info(f"Writing feature data into Databricks Feature Store table {full_table_name}")

        self.__feature_store_client.write_table(name=full_table_name, df=df, mode="overwrite")

        self.__logger.info("Features write done")
