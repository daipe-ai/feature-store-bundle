from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from logging import Logger
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.delta.metadata.schema import get_metadata_schema
from featurestorebundle.metadata.reader.MetadataReaderInterface import MetadataReaderInterface


class DeltaTableMetadataReader(MetadataReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        spark: SparkSession,
        table_existence_checker: TableExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__table_existence_checker = table_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator

    def read(self, entity_name: Optional[str]) -> DataFrame:
        entity_name = entity_name or ""
        full_table_name = self.__table_names.get_metadata_full_table_name(entity_name)

        if not self.__table_existence_checker.exists(full_table_name):
            self.__logger.debug(f"Metadata does not exist in hive {full_table_name}, returning empty metadata dataframe")

            return self.__empty_dataframe_creator.create(get_metadata_schema())

        self.__logger.info(f"Reading metadata from hive table {full_table_name}")

        return self.__spark.read.table(full_table_name)
