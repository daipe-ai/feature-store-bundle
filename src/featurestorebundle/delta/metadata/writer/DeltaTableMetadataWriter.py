from pyspark.sql import SparkSession
from featurestorebundle.metadata.writer.MetadataWriterInterface import MetadataWriterInterface
from featurestorebundle.delta.metadata.writer.DeltaTableMetadataPreparer import DeltaTableMetadataPreparer
from featurestorebundle.delta.metadata.writer.DeltaMetadataHandler import DeltaMetadataHandler
from featurestorebundle.delta.metadata.schema import get_metadata_schema
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.db.TableNames import TableNames


class DeltaTableMetadataWriter(MetadataWriterInterface):
    def __init__(
        self,
        spark: SparkSession,
        table_names: TableNames,
        table_preparer: DeltaTableMetadataPreparer,
        metadata_handler: DeltaMetadataHandler,
    ):
        self.__spark = spark
        self.__table_names = table_names
        self.__table_preparer = table_preparer
        self.__metadata_handler = metadata_handler

    def write(self, feature_list: FeatureList):
        full_table_name = self.__table_names.get_metadata_full_table_name()
        path = self.__table_names.get_metadata_path()
        metadata_df = self.__spark.createDataFrame(feature_list.get_metadata(), get_metadata_schema())

        self.__table_preparer.prepare(full_table_name, path)
        self.__metadata_handler.merge_to_delta_table(full_table_name, metadata_df)
