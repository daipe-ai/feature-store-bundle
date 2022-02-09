from typing import Union, Optional
from logging import Logger
from pyspark.sql.types import StructType
from featurestorebundle.delta.PathExistenceChecker import PathExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator


class PathCreator:
    def __init__(
        self,
        logger: Logger,
        path_existence_checker: PathExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
    ):
        self.__logger = logger
        self.__path_existence_checker = path_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator

    def create_if_not_exists(self, path: str, schema: StructType, partition_by: Optional[Union[str, list]] = None):
        partition_by = partition_by or []
        partition_by = [partition_by] if isinstance(partition_by, str) else partition_by

        if self.__path_existence_checker.exists(path):
            self.__logger.info(f"Delta table at path {path} already exists, creation skipped")
            return

        self.__logger.info(f"Creating new delta table at path {path}")

        df = self.__empty_dataframe_creator.create(schema)

        df.write.partitionBy(*partition_by).format("delta").save(path)

        self.__logger.info("Delta table successfully created")
