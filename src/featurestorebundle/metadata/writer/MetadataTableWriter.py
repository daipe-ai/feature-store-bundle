from pyspark.sql import SparkSession
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.widgets.WidgetsGetter import WidgetsGetter
from featurestorebundle.metadata.writer.MetadataTablePreparer import MetadataTablePreparer
from featurestorebundle.metadata.writer.MetadataTableDataHandler import MetadataTableDataHandler
from featurestorebundle.metadata.schema import get_metadata_schema


class MetadataTableWriter:
    def __init__(
        self,
        spark: SparkSession,
        table_names: TableNames,
        table_preparer: MetadataTablePreparer,
        metadata_handler: MetadataTableDataHandler,
        widgets_getter: WidgetsGetter,
    ):
        self.__spark = spark
        self.__table_names = table_names
        self.__table_preparer = table_preparer
        self.__metadata_handler = metadata_handler
        self.__widgets_getter = widgets_getter

    def write(self, feature_list: FeatureList):
        entity = feature_list.entity
        full_table_name = self.__get_metadata_table(entity)
        path = self.__get_metadata_path(entity)
        metadata_df = self.__spark.createDataFrame(feature_list.get_metadata(), get_metadata_schema())

        self.__table_preparer.prepare(full_table_name, path)
        self.__metadata_handler.merge(full_table_name, metadata_df)

    def __get_metadata_table(self, entity: Entity) -> str:
        return (
            self.__table_names.get_latest_metadata_full_table_name(entity.name)
            if self.__widgets_getter.timestamp_exists()
            else self.__table_names.get_target_metadata_full_table_name(entity.name, self.__widgets_getter.get_target())
        )

    def __get_metadata_path(self, entity: Entity) -> str:
        return (
            self.__table_names.get_latest_metadata_path(entity.name)
            if self.__widgets_getter.timestamp_exists()
            else self.__table_names.get_target_metadata_path(entity.name, self.__widgets_getter.get_target())
        )
