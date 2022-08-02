from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from logging import Logger
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.PathExistenceChecker import PathExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.delta.metadata.schema import get_metadata_schema
from featurestorebundle.metadata.reader.MetadataReaderInterface import MetadataReaderInterface


class DeltaPathMetadataReader(MetadataReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        spark: SparkSession,
        path_existence_checker: PathExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__path_existence_checker = path_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator

    def read(self, entity_name: Optional[str]) -> DataFrame:
        entity_name = entity_name or ""
        path = self.__table_names.get_metadata_path(entity_name)

        if not self.__path_existence_checker.exists(path):
            self.__logger.debug(f"Metadata does not exist at path {path}, returning empty metadata dataframe")

            return self.__empty_dataframe_creator.create(get_metadata_schema())

        self.__logger.info(f"Reading metadata from path {path}")

        return self.__spark.read.format("delta").load(path)
