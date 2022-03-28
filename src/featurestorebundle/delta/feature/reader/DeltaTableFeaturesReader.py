from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


class DeltaTableFeaturesReader(FeaturesReaderInterface):
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_names: TableNames,
        table_existence_checker: TableExistenceChecker,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__table_existence_checker = table_existence_checker

    def read(self, entity_name: str) -> DataFrame:
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)

        self.__logger.info(f"Reading features from table {full_table_name}")

        return self.__spark.read.table(full_table_name)

    def exists(self, entity_name: str) -> bool:
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)

        return self.__table_existence_checker.exists(full_table_name)
