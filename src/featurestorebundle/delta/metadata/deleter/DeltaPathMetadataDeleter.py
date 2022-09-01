from typing import List
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.metadata.deleter.MetadataDeleterInterface import MetadataDeleterInterface


class DeltaPathMetadataDeleter(MetadataDeleterInterface):
    def __init__(
        self,
        spark: SparkSession,
        table_names: TableNames,
        entity_getter: EntityGetter,
    ):
        self.__spark = spark
        self.__table_names = table_names
        self.__entity_getter = entity_getter

    def delete(self, features: List[str]):
        entity = self.__entity_getter.get()
        path = self.__table_names.get_metadata_path(entity.name)
        metadata = self.__spark.read.table(path)
        metadata = metadata.filter(~f.col("feature").isin(features))
        metadata.write.format("delta").mode("overwrite").saveAsTable(path)
