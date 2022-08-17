from typing import Optional
from box import Box
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.metadata.reader.MetadataReaderInterface import MetadataReaderInterface


class MetadataReader:
    def __init__(
        self,
        stable_feature_store: Box,
        metadata_reader: MetadataReaderInterface,
        spark: SparkSession,
    ):
        self.__stable_feature_store = stable_feature_store
        self.__metadata_reader = metadata_reader
        self.__spark = spark

    def read(self, entity_name: Optional[str]) -> DataFrame:
        return self.__metadata_reader.read(entity_name)

    def read_for_latest(self, entity_name: Optional[str]) -> DataFrame:
        if self.__stable_feature_store.metadata.table is None:
            return self.__read_current_feature_store_metadata(entity_name)

        return self.__read_current_and_stable_feature_store_metadata(entity_name)

    def read_for_target(self, entity_name: Optional[str], target_name: str) -> DataFrame:
        if (
            self.__stable_feature_store.metadata.table is None
            or self.__stable_feature_store.target.table is None
            or self.__stable_feature_store.target.enum_table is None
        ):
            return self.__read_current_feature_store_metadata(entity_name)

        stable_targets = [
            row.target_id for row in self.__spark.read.table(self.__stable_feature_store.target.enum_table).select("target_id").collect()
        ]

        if target_name in stable_targets:
            return self.__read_stable_feature_store_metadata()

        return self.__read_current_feature_store_metadata(entity_name)

    def __read_current_feature_store_metadata(self, entity_name: Optional[str]) -> DataFrame:
        return self.__metadata_reader.read(entity_name)

    def __read_stable_feature_store_metadata(self) -> DataFrame:
        return self.__spark.read.table(self.__stable_feature_store.metadata.table)

    def __read_current_and_stable_feature_store_metadata(self, entity_name: Optional[str]) -> DataFrame:
        current_feature_store_metadata = self.__read_current_feature_store_metadata(entity_name)
        stable_feature_store_metadata = self.__read_stable_feature_store_metadata()

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
