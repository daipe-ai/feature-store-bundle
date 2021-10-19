from logging import Logger
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.metadata.MetadataWriter import MetadataWriter
from featurestorebundle.databricks.DatabricksMergeConfig import DatabricksMergeConfig


class DatabricksDataHandler:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_names: TableNames,
        metadata_writer: MetadataWriter,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__metadata_writer = metadata_writer

    def write(self, data_table: str, metadata_table: str, config: DatabricksMergeConfig, feature_list: FeatureList):
        self.__merge(data_table, config)
        self.__metadata_writer.write(metadata_table, feature_list)

    def __merge(self, full_table_name: str, config: DatabricksMergeConfig):
        try:
            from databricks import feature_store

        except ImportError:
            raise Exception("Cannot import Databricks Feature Store, you need to use ML cluster with DBR 8.3+")

        dbx_feature_store_client = feature_store.FeatureStoreClient()
        db_name = full_table_name.split(".")[0]

        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        features_data = self.__replace_decimal_types(config.data)

        if not self.__table_exists(dbx_feature_store_client, full_table_name):
            dbx_feature_store_client.create_feature_table(
                name=full_table_name, keys=config.pk_columns, schema=features_data.select(config.pk_columns).schema
            )

        self.__logger.info(f"Writing feature data into {full_table_name}")

        dbx_feature_store_client.write_table(
            name=full_table_name,
            df=features_data,
            mode="merge",
        )

    def __table_exists(self, dbx_feature_store_client, full_table_name: str):
        try:
            dbx_feature_store_client.get_feature_table(full_table_name)
            return True

        except Exception:
            return False

    def __replace_decimal_types(self, features_data: DataFrame) -> DataFrame:
        for field in features_data.schema:
            if isinstance(field.dataType, t.DecimalType):
                if field.dataType.precision == 0:
                    features_data = features_data.withColumn(field.name, f.col(field.name).cast(t.LongType()))
                else:
                    features_data = features_data.withColumn(field.name, f.col(field.name).cast(t.DoubleType()))

        return features_data
