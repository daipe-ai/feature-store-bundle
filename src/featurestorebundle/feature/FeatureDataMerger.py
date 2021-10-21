from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t
from delta.tables import DeltaTable
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from logging import Logger

from featurestorebundle.metadata.MetadataWriter import MetadataWriter


class FeatureDataMerger:
    def __init__(self, logger: Logger, spark: SparkSession, metadata_writer: MetadataWriter):
        self.__logger = logger
        self.__spark = spark
        self.__metadata_writer = metadata_writer

    def merge(
        self,
        entity: Entity,
        feature_list: FeatureList,
        features_data: DataFrame,
        pk_columns: list,
        target_table_path: str,
        metadata_table_path: str,
    ):
        feature_names = feature_list.get_names()

        data_column_names = [
            field.name for field in features_data.schema.fields if field.name not in [entity.id_column, entity.time_column]
        ]

        if len(data_column_names) != len(feature_names):
            raise Exception(
                f"Number or dataframe columns ({len(data_column_names)}) != number of features instances matched ({len(feature_names)})"
            )

        def build_update_set():
            update_set = {}

            for i, feature_instance_name in enumerate(feature_names):
                update_set[feature_instance_name] = f"source.{data_column_names[i]}"

            return update_set

        def build_insert_set():
            insert_set = build_update_set()
            insert_set[entity.id_column] = f"source.{entity.id_column}"
            insert_set[entity.time_column] = f"source.{entity.time_column}"

            return insert_set

        def build_merge_condition():
            conditions = []

            for pk in pk_columns:
                conditions.append(f"target.{pk} = source.{pk}")

            return " AND ".join(conditions)

        delta_table = DeltaTable.forPath(self.__spark, target_table_path)

        self.__logger.info(f"Writing feature data into {target_table_path}")

        (
            delta_table.alias("target")
            .merge(features_data.alias("source"), build_merge_condition())
            .whenMatchedUpdate(set=build_update_set())
            .whenNotMatchedInsert(values=build_insert_set())
            .execute()
        )

        self.__metadata_writer.write(metadata_table_path, feature_list)

