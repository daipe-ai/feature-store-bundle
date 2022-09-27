from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class FeaturesTableReader:
    def __init__(self, logger: Logger, spark: SparkSession):
        self.__logger = logger
        self.__spark = spark

    def read(self, full_table_name: str) -> DataFrame:
        self.__logger.info(f"Reading features from hive table {full_table_name}")

        return self.__spark.read.table(full_table_name)
