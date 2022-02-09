from logging import Logger
from pyspark.sql import SparkSession
from delta import DeltaTable
from featurestorebundle.delta.feature.writer.DeltaFeaturesMergeConfig import DeltaFeaturesMergeConfig


class DeltaFeaturesDataHandler:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
    ):
        self.__logger = logger
        self.__spark = spark

    def merge_to_delta_table(self, full_table_name: str, config: DeltaFeaturesMergeConfig):
        delta_table = DeltaTable.forName(self.__spark, full_table_name)

        self.__logger.info(f"Writing features into {full_table_name}")

        self.__delta_merge(delta_table, config)

        self.__logger.info("Features write done")

    def merge_to_delta_path(self, path: str, config: DeltaFeaturesMergeConfig):
        delta_table = DeltaTable.forPath(self.__spark, path)

        self.__logger.info(f"Writing features into {path}")

        self.__delta_merge(delta_table, config)

        self.__logger.info("Features write done")

    def __delta_merge(self, delta_table: DeltaTable, config: DeltaFeaturesMergeConfig):  # noqa
        (
            delta_table.alias(config.target)
            .merge(config.data.alias(config.source), config.merge_condition)
            .whenMatchedUpdate(set=config.update_set)
            .whenNotMatchedInsert(values=config.insert_set)
            .execute()
        )
