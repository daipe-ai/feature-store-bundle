from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from logging import Logger
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface


class DeltaTableTargetsReader(TargetsReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        spark: SparkSession,
        table_existence_checker: TableExistenceChecker,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__table_existence_checker = table_existence_checker

    def read(self, entity_name: str) -> DataFrame:
        full_table_name = self.__table_names.get_targets_full_table_name(entity_name)

        self.__logger.info(f"Reading targets for entity {entity_name} from table {full_table_name}")

        return self.__spark.read.table(full_table_name)

    def read_enum(self) -> DataFrame:
        full_table_name = self.__table_names.get_targets_enum_full_table_name()

        return self.__spark.read.table(full_table_name)

    def exists(self, entity_name: str) -> bool:
        full_table_name = self.__table_names.get_targets_full_table_name(entity_name)

        return self.__table_existence_checker.exists(full_table_name)
