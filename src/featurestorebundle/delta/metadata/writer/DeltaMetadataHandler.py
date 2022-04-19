from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta import DeltaTable
from featurestorebundle.delta.metadata.schema import get_metadata_pk_columns, get_metadata_columns


class DeltaMetadataHandler:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
    ):
        self.__logger = logger
        self.__spark = spark

    def merge_to_delta_table(self, full_table_name: str, metadata_df: DataFrame):
        delta_table = DeltaTable.forName(self.__spark, full_table_name)

        self.__logger.info(f"Writing metadata into {full_table_name}")

        self.__delta_merge(delta_table, metadata_df)

        self.__logger.info("Metadata write done")

    def merge_to_delta_path(self, path: str, metadata_df: DataFrame):
        delta_table = DeltaTable.forPath(self.__spark, path)

        self.__logger.info(f"Writing metadata into {path}")

        self.__delta_merge(delta_table, metadata_df)

        self.__logger.info("Metadata write done")

    def __delta_merge(self, delta_table: DeltaTable, metadata_df: DataFrame):  # noqa
        update_set = {col.name: f"source.{col.name}" for col in get_metadata_columns()}
        insert_set = {**{col.name: f"source.{col.name}" for col in get_metadata_pk_columns()}, **update_set}
        merge_condition = " AND ".join(f"target.{col.name} = source.{col.name}" for col in get_metadata_pk_columns())
        last_compute_date_condition = "source.last_compute_date > target.last_compute_date OR target.last_compute_date <=> NULL"
        update_set_without_last_compute_date = {col: update_set[col] for col in update_set if col != "last_compute_date"}

        (
            delta_table.alias("target")
            .merge(metadata_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set, condition=last_compute_date_condition)
            .whenMatchedUpdate(set=update_set_without_last_compute_date)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )
