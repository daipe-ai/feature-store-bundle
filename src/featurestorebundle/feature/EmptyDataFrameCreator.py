from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity


class EmptyDataFrameCreator:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def create(self, entity: Entity) -> DataFrame:
        return self.__spark.createDataFrame(self.__spark.sparkContext.emptyRDD(), schema=self.__get_dataframe_schema(entity))

    def __get_dataframe_schema(self, entity: Entity) -> str:
        return f"{entity.id_column} {entity.id_column_type}, {entity.time_column} {entity.time_column_type}"
