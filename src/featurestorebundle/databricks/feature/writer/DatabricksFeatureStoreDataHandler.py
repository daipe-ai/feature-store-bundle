from logging import Logger
from featurestorebundle.databricks.DatabricksFeatureStoreClientFactory import DatabricksFeatureStoreClientFactory
from featurestorebundle.databricks.feature.writer.DatabricksFeatureStoreMergeConfig import DatabricksFeatureStoreMergeConfig


class DatabricksFeatureStoreDataHandler:
    def __init__(
        self,
        logger: Logger,
        feature_store_client_factory: DatabricksFeatureStoreClientFactory,
    ):
        self.__logger = logger
        self.__feature_store_client = feature_store_client_factory.create()

    def merge_to_databricks_feature_store(self, full_table_name: str, config: DatabricksFeatureStoreMergeConfig):
        self.__logger.info(f"Writing feature data into Databricks Feature Store table {full_table_name}")

        self.__feature_store_client.write_table(name=full_table_name, df=config.data, mode="merge")

        self.__logger.info("Features write done")
