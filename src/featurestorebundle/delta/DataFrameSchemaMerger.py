from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator


class DataFrameSchemaMerger:
    def __init__(self, empty_dataframe_creator: EmptyDataFrameCreator):
        self.__empty_dataframe_creator = empty_dataframe_creator

    def merge(self, dataframe: DataFrame, schema: StructType) -> DataFrame:
        return dataframe.unionByName(self.__empty_dataframe_creator.create(schema), allowMissingColumns=True)
