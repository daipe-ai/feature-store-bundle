from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.PathExistenceChecker import PathExistenceChecker
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


class DeltaPathFeaturesReader(FeaturesReaderInterface):
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_names: TableNames,
        path_existence_checker: PathExistenceChecker,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__path_existence_checker = path_existence_checker

    def read(self, entity_name: str) -> DataFrame:
        path = self.__table_names.get_features_path(entity_name)

        self.__logger.info(f"Reading features from path {path}")

        return self.__spark.read.format("delta").load(path)

    def exists(self, entity_name: str) -> bool:
        path = self.__table_names.get_features_path(entity_name)

        return self.__path_existence_checker.exists(path)
