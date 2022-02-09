from pyspark.sql import SparkSession
from delta import DeltaTable


class PathExistenceChecker:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def exists(self, path: str) -> bool:
        return DeltaTable.isDeltaTable(self.__spark, path)
