from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


class DeltaTableFeaturesReader(FeaturesReaderInterface):
    def __init__(self, logger: Logger, spark: SparkSession):
        self.__logger = logger
        self.__spark = spark

    def read(self, location: str) -> DataFrame:
        self.__logger.info(f"Reading features from hive table {location}")

        return self.__spark.read.table(location)

    def get_backend(self) -> str:
        return "delta_table"
