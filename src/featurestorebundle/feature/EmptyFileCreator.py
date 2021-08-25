from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.EmptyDataFrameCreator import EmptyDataFrameCreator


class EmptyFileCreator:
    def __init__(self, spark: SparkSession, empty_dataframe_creator: EmptyDataFrameCreator):
        self.__spark = spark
        self.__empty_dataframe_creator = empty_dataframe_creator

    def create(self, path: str, entity: Entity):
        if not DeltaTable.isDeltaTable(self.__spark, path):
            df = self.__empty_dataframe_creator.create(entity)
            df.write.format("delta").save(path)
