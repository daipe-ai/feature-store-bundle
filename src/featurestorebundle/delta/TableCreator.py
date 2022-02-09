from typing import Union, Optional
from logging import Logger
from pyspark.sql.types import StructType
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator


class TableCreator:
    def __init__(
        self,
        logger: Logger,
        table_existence_checker: TableExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
    ):
        self.__logger = logger
        self.__table_existence_checker = table_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator

    def create_if_not_exists(self, full_table_name: str, path: str, schema: StructType, partition_by: Optional[Union[str, list]] = None):
        partition_by = partition_by or []
        partition_by = [partition_by] if isinstance(partition_by, str) else partition_by

        if self.__table_existence_checker.exists(full_table_name):
            self.__logger.info(f"Table {full_table_name} already exists, creation skipped")
            return

        self.__logger.info(f"Creating new table {full_table_name} for {path}")

        df = self.__empty_dataframe_creator.create(schema)

        df.write.partitionBy(*partition_by).format("delta").option("path", path).saveAsTable(f"{full_table_name}")

        self.__logger.info(f"Table {full_table_name} successfully created")
