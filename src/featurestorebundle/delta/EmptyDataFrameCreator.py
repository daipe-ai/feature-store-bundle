from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class EmptyDataFrameCreator:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def create(self, schema: StructType) -> DataFrame:
        return self.__spark.createDataFrame(self.__spark.sparkContext.emptyRDD(), schema=schema)
