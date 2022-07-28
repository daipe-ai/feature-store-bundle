from typing import List, Union, Optional
from logging import Logger
from pyspark.sql.types import StructType
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.delta.TablePropertiesSetter import TablePropertiesSetter


class TableCreator:
    def __init__(
        self,
        logger: Logger,
        table_existence_checker: TableExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
        table_properties_setter: TablePropertiesSetter,
    ):
        self.__logger = logger
        self.__table_existence_checker = table_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator
        self.__table_properties_setter = table_properties_setter

    def create_if_not_exists(
        self, full_table_name: str, path: str, schema: StructType, partition_by: Optional[Union[str, List[str]]] = None
    ):
        partition_by = partition_by or []
        partition_by = [partition_by] if isinstance(partition_by, str) else partition_by

        if self.__table_existence_checker.exists(full_table_name):
            self.__logger.info(f"Table {full_table_name} already exists, creation skipped")
            return

        self.__logger.info(f"Creating new table {full_table_name} for {path}")

        df = self.__empty_dataframe_creator.create(schema)

        df.write.partitionBy(*partition_by).format("delta").option("path", path).saveAsTable(full_table_name)

        self.__set_delta_name_column_mapping_mode(full_table_name)

        self.__logger.info(f"Table {full_table_name} successfully created")

    def __set_delta_name_column_mapping_mode(self, full_table_name: str):
        self.__table_properties_setter.set_properties(
            table_identifier=full_table_name,
            properties={
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
                "delta.columnMapping.mode": "name",
            },
        )
