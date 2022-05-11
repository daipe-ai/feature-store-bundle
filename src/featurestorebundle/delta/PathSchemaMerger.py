from pyspark.sql.types import StructType
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator


class PathSchemaMerger:
    def __init__(self, empty_dataframe_creator: EmptyDataFrameCreator):
        self.__empty_dataframe_creator = empty_dataframe_creator

    def merge(self, path: str, new_schema: StructType):
        new_df = self.__empty_dataframe_creator.create(new_schema)

        new_df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
