from logging import Logger
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.metadata.schema import (
    METADATA_PK,
    METADATA_COLUMNS,
    METADATA_SCHEMA,
)


class MetadataWriter:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
    ):
        self.__logger = logger
        self.__spark = spark

    def write(self, full_table_name: str, feature_list: FeatureList):
        metadata_df = self.__spark.createDataFrame(feature_list.get_metadata(), METADATA_SCHEMA)

        delta_table = DeltaTable.forName(self.__spark, full_table_name)

        update_set = {col.name: f"source.{col.name}" for col in METADATA_COLUMNS}

        insert_set = {METADATA_PK.name: f"source.{METADATA_PK.name}", **update_set}

        self.__logger.info(f"Writing metadata into {full_table_name}")

        (
            delta_table.alias("target")
            .merge(metadata_df.alias("source"), "target.feature = source.feature")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )
