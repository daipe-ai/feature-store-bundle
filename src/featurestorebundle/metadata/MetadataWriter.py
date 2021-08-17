from logging import Logger

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import types as t

from featurestorebundle.feature.FeatureList import FeatureList

METADATA_PK = t.StructField("feature", t.StringType(), True)
METADATA_COLUMNS = [
    t.StructField("description", t.StringType(), True),
    t.StructField("extra", t.MapType(t.StringType(), t.StringType(), True)),
    t.StructField("feature_template", t.StringType(), True),
    t.StructField("category", t.StringType(), True),
    t.StructField("dtype", t.StringType(), True),
]
METADATA_SCHEMA = t.StructType([METADATA_PK] + METADATA_COLUMNS)


class MetadataWriter:
    def __init__(self, logger: Logger, spark: SparkSession):
        self.__logger = logger
        self.__spark = spark

    def write(self, metadata_path: str, feature_list: FeatureList):
        metadata_df = self.__spark.createDataFrame(feature_list.get_metadata(), METADATA_SCHEMA)

        if not DeltaTable.isDeltaTable(self.__spark, metadata_path):
            metadata_df.write.format("delta").save(metadata_path)
            return

        delta_table = DeltaTable.forPath(self.__spark, metadata_path)

        update_set = {col.name: f"source.{col.name}" for col in METADATA_COLUMNS}

        insert_set = {METADATA_PK.name: f"source.{METADATA_PK.name}", **update_set}

        self.__logger.info(f"Writing metadata into {metadata_path}")

        (
            delta_table.alias("target")
            .merge(metadata_df.alias("source"), "target.feature = source.feature")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )
