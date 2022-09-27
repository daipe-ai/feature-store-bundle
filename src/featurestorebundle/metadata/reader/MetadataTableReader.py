from logging import Logger
from box import Box
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.dataframe.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.table.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.metadata.schema import get_metadata_schema


class MetadataTableReader:
    def __init__(
        self,
        logger: Logger,
        stable_feature_store: Box,
        spark: SparkSession,
        table_names: TableNames,
        table_existence_checker: TableExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
    ):
        self.__logger = logger
        self.__stable_feature_store = stable_feature_store
        self.__spark = spark
        self.__table_names = table_names
        self.__table_existence_checker = table_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator

    def read(self, entity_name: str) -> DataFrame:
        return self.__read_latest_current_feature_store_metadata(entity_name)

    def read_for_latest(self, entity_name: str) -> DataFrame:
        if self.__stable_feature_store.metadata.base_db is None:
            return self.__read_latest_current_feature_store_metadata(entity_name)

        return self.__read_latest_current_and_stable_feature_store_metadata(entity_name)

    def read_for_target(self, entity_name: str, target: str) -> DataFrame:
        if self.__stable_feature_store.metadata.base_db is None or self.__stable_feature_store.target.base_db is None:
            return self.__read_target_current_feature_store_metadata(entity_name, target)

        stable_targets = [
            row.target_id for row in self.__spark.read.table(self.__stable_feature_store.target.enum_table).select("target_id").collect()
        ]

        if target in stable_targets:
            return self.__read_target_stable_feature_store_metadata(entity_name, target)

        return self.__read_target_current_feature_store_metadata(entity_name, target)

    def __read_latest_current_feature_store_metadata(self, entity_name: str) -> DataFrame:
        full_table_name = self.__table_names.get_latest_metadata_full_table_name(entity_name)

        self.__logger.info(f"Reading metadata from hive table {full_table_name}")

        if not self.__table_existence_checker.exists(full_table_name):
            self.__logger.debug(f"Metadata does not exist in hive {full_table_name}, returning empty metadata dataframe")

            return self.__empty_dataframe_creator.create(get_metadata_schema())

        return self.__spark.read.table(full_table_name)

    def __read_target_current_feature_store_metadata(self, entity_name: str, target: str) -> DataFrame:
        full_table_name = self.__table_names.get_target_metadata_full_table_name(entity_name, target)

        self.__logger.info(f"Reading metadata from hive table {full_table_name}")

        if not self.__table_existence_checker.exists(full_table_name):
            self.__logger.debug(f"Metadata does not exist in hive {full_table_name}, returning empty metadata dataframe")

            return self.__empty_dataframe_creator.create(get_metadata_schema())

        return self.__spark.read.table(full_table_name)

    def __read_latest_stable_feature_store_metadata(self, entity_name: str) -> DataFrame:
        full_table_name = self.__table_names.get_latest_metadata_full_table_name_for_base_db(
            self.__stable_feature_store.metadata.base_db, entity_name
        )

        self.__logger.info(f"Reading stable feature store metadata from hive table {full_table_name}")

        return self.__spark.read.table(full_table_name)

    def __read_target_stable_feature_store_metadata(self, entity_name: str, target: str) -> DataFrame:
        full_table_name = self.__table_names.get_target_metadata_full_table_name_for_base_db(
            self.__stable_feature_store.metadata.base_db, entity_name, target
        )

        self.__logger.info(f"Reading stable feature store metadata from hive table {full_table_name}")

        return self.__spark.read.table(full_table_name)

    def __read_latest_current_and_stable_feature_store_metadata(self, entity_name: str) -> DataFrame:
        current_feature_store_metadata = self.__read_latest_current_feature_store_metadata(entity_name)
        stable_feature_store_metadata = self.__read_latest_stable_feature_store_metadata(entity_name)

        return self.__merge_current_and_stable_metadata(current_feature_store_metadata, stable_feature_store_metadata)

    def __merge_current_and_stable_metadata(
        self, current_feature_store_metadata: DataFrame, stable_feature_store_metadata: DataFrame
    ) -> DataFrame:
        stable_features = stable_feature_store_metadata.select("feature")
        current_features = current_feature_store_metadata.select("feature")
        new_features = current_features.exceptAll(stable_features)

        return stable_feature_store_metadata.unionByName(
            new_features.join(current_feature_store_metadata, on=["feature"], how="left"), allowMissingColumns=True
        )
