from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta import DeltaTable
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.PathCreator import PathCreator
from featurestorebundle.delta.PathExistenceChecker import PathExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.delta.feature.schema import (
    get_rainbow_table_hash_column,
    get_rainbow_table_features_column,
    get_rainbow_table_schema,
)


class DeltaRainbowTableManager:
    def __init__(
        self,
        spark: SparkSession,
        table_names: TableNames,
        path_creator: PathCreator,
        path_existence_checker: PathExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
    ):
        self.__spark = spark
        self.__table_names = table_names
        self.__path_creator = path_creator
        self.__path_existence_checker = path_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator

    def read(self, entity_name: str) -> DataFrame:
        return self.__spark.read.format("delta").load(self.__table_names.get_rainbow_path(entity_name))

    def read_safe(self, entity_name: str):
        if not self.exists(entity_name):
            return self.__empty_dataframe_creator.create(get_rainbow_table_schema())

        return self.__spark.read.format("delta").load(self.__table_names.get_rainbow_path(entity_name))

    def exists(self, entity_name: str) -> bool:
        return self.__path_existence_checker.exists(self.__table_names.get_rainbow_path(entity_name))

    def merge(self, entity_name: str, df: DataFrame):
        path = self.__table_names.get_rainbow_path(entity_name)

        self.__path_creator.create_if_not_exists(path, get_rainbow_table_schema())

        delta_table = DeltaTable.forPath(self.__spark, path)

        update_set = {get_rainbow_table_features_column().name: f"source.{get_rainbow_table_features_column().name}"}
        insert_set = {get_rainbow_table_hash_column().name: f"source.{get_rainbow_table_hash_column().name}", **update_set}
        merge_condition = f"target.{get_rainbow_table_hash_column().name} = source.{get_rainbow_table_hash_column().name}"

        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )
