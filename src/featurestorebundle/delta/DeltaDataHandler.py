import datetime as dt
from delta import DeltaTable
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.DeltaMergeConfig import DeltaMergeConfig
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.metadata.MetadataWriter import MetadataWriter
from logging import Logger
from pyspark.sql import SparkSession


class DeltaDataHandler:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        feature_store: FeatureStore,
        table_names: TableNames,
        metadata_writer: MetadataWriter,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__feature_store = feature_store
        self.__table_names = table_names
        self.__metadata_writer = metadata_writer

    def write(self, data_table: str, metadata_table: str, config: DeltaMergeConfig, feature_list: FeatureList):
        self.__delta_merge(data_table, config)
        self.__metadata_writer.write(metadata_table, feature_list)

    def __delta_merge(self, full_table_name: str, config: DeltaMergeConfig):
        delta_table = DeltaTable.forName(self.__spark, full_table_name)

        self.__logger.info(f"Writing feature data into {full_table_name}")

        (
            delta_table.alias(config.target)
            .merge(config.data.alias(config.source), config.merge_condition)
            .whenMatchedUpdate(set=config.update_set)
            .whenNotMatchedInsert(values=config.insert_set)
            .execute()
        )

    def archive(self, entity: Entity):
        today_str = dt.date.today().strftime("%Y-%m-%d")
        archive_path = self.__table_names.get_archive_path(entity.name, today_str)
        feature_store_df = self.__feature_store.get_latest(entity.name)
        feature_store_df.write.format("delta").save(archive_path)
