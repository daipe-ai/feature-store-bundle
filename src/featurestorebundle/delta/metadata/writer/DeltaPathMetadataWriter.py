from pyspark.sql import SparkSession
from featurestorebundle.metadata.writer.MetadataWriterInterface import MetadataWriterInterface
from featurestorebundle.delta.metadata.writer.DeltaPathMetadataPreparer import DeltaPathMetadataPreparer
from featurestorebundle.delta.metadata.writer.DeltaMetadataHandler import DeltaMetadataHandler
from featurestorebundle.delta.metadata.schema import get_metadata_schema
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.db.TableNames import TableNames


class DeltaPathMetadataWriter(MetadataWriterInterface):
    def __init__(
        self,
        spark: SparkSession,
        table_names: TableNames,
        path_preparer: DeltaPathMetadataPreparer,
        metadata_handler: DeltaMetadataHandler,
    ):
        self.__spark = spark
        self.__table_names = table_names
        self.__path_preparer = path_preparer
        self.__metadata_handler = metadata_handler

    def write(self, entity: Entity, feature_list: FeatureList):
        path = self.__table_names.get_metadata_path(entity.name)
        metadata_df = self.__spark.createDataFrame(feature_list.get_metadata(), get_metadata_schema())

        self.__path_preparer.prepare(path)
        self.__metadata_handler.merge_to_delta_path(path, metadata_df)
