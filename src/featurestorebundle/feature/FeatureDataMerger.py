from delta.tables import DeltaTable
from featurestorebundle.entity.Entity import Entity
from logging import Logger
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict


class FeatureDataMerger:
    def __init__(self, logger: Logger, spark: SparkSession):
        self.__logger = logger
        self.__spark = spark

    def merge(
        self,
        entity: Entity,
        features_data: DataFrame,
        pk_columns: List[str],
        target_table_path: str,
    ):
        data_column_names = [
            field.name for field in features_data.schema.fields if field.name not in [entity.id_column, entity.time_column]
        ]

        update_set = self.__build_set(data_column_names)
        if entity.time_column not in pk_columns:
            update_set[entity.time_column] = self.__wrap_source(entity.time_column)

        insert_set = {**update_set, **self.__build_set([entity.id_column, entity.time_column])}
        merge_condition = " AND ".join(f"target.{pk} = source.{pk}" for pk in pk_columns)

        delta_table = DeltaTable.forPath(self.__spark, target_table_path)

        self.__logger.info(f"Writing feature data into {target_table_path}")

        (
            delta_table.alias("target")
            .merge(features_data.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )

    def __wrap_source(self, column: str) -> str:
        return f"source.{column}"

    def __build_set(self, columns: List[str]) -> Dict[str, str]:
        return {column: self.__wrap_source(column) for column in columns}
