from typing import List
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.metadata.deleter.MetadataDeleterInterface import MetadataDeleterInterface


class DeltaTableMetadataDeleter(MetadataDeleterInterface):
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
        full_table_name = self.__table_names.get_metadata_full_table_name(entity.name)
        metadata = self.__spark.read.table(full_table_name)
        metadata = metadata.filter(~f.col("feature").isin(features))
        metadata.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
