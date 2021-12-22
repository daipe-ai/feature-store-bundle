import datetime as dt
from featurestorebundle.delta.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.metadata.MetadataWriter import MetadataWriter
from pyspark.sql import DataFrame
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.feature.FeatureDataMerger import FeatureDataMerger
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.TablePreparer import TablePreparer
from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface
from featurestorebundle.db.TableNames import TableNames


class DeltaWriter(FeaturesWriterInterface):
    def __init__(
        self,
        features_preparer: FeaturesPreparer,
        feature_store: FeatureStore,
        table_preparer: TablePreparer,
        feature_data_merger: FeatureDataMerger,
        table_names: TableNames,
        metadata_writer: MetadataWriter,
    ):
        self.__features_preparer = features_preparer
        self.__feature_store = feature_store
        self.__table_preparer = table_preparer
        self.__feature_data_merger = feature_data_merger
        self.__table_names = table_names
        self.__metadata_writer = metadata_writer

    def write_latest(self, features_storage: FeaturesStorage, archive=False):
        features_data = self.__features_preparer.prepare(features_storage)

        self.write_dataframe_latest(features_data, features_storage.feature_list, features_storage.entity, archive)

    def write_dataframe_latest(self, features_data: DataFrame, feature_list: FeatureList, entity: Entity, archive=False):
        self.__check_features_validity(entity, features_data, feature_list)

        table_identifier = self.__table_names.get_latest_table_identifier(entity.name)
        path = self.__table_names.get_latest_path(entity.name)
        metadata_path = self.__table_names.get_latest_metadata_path(entity.name)
        pk_columns = [entity.id_column]

        if archive:
            today_str = dt.date.today().strftime("%Y-%m-%d")
            archive_path = self.__table_names.get_archive_path(entity.name, today_str)
            feature_store_df = self.__feature_store.get_latest(entity.name)
            feature_store_df.write.format("delta").save(archive_path)

        self.__table_preparer.prepare(table_identifier, path, entity, feature_list)

        self.__feature_data_merger.merge(
            entity,
            features_data,
            pk_columns,
            path,
        )

        self.__metadata_writer.write(metadata_path, feature_list)

    def write_historized(self, features_storage: FeaturesStorage):
        features_data = self.__features_preparer.prepare(features_storage)

        self.write_dataframe_historized(features_data, features_storage.feature_list, features_storage.entity)

    def write_dataframe_historized(self, features_data: DataFrame, feature_list: FeatureList, entity: Entity):
        self.__check_features_validity(entity, features_data, feature_list)

        table_identifier = self.__table_names.get_historized_table_identifier(entity.name)
        path = self.__table_names.get_historized_path(entity.name)
        metadata_path = self.__table_names.get_historized_metadata_path(entity.name)
        pk_columns = [entity.id_column, entity.time_column]

        self.__table_preparer.prepare(table_identifier, path, entity, feature_list)

        self.__feature_data_merger.merge(
            entity,
            features_data,
            pk_columns,
            path,
        )

        self.__metadata_writer.write(metadata_path, feature_list)

    def __check_features_validity(self, entity: Entity, features_data: DataFrame, feature_list: FeatureList):
        data_column_names = [
            field.name for field in features_data.schema.fields if field.name not in [entity.id_column, entity.time_column]
        ]
        feature_names = feature_list.get_names()

        if data_column_names != feature_names:
            raise Exception(f"Dataframe columns of size ({len(data_column_names)}) != features matched of size ({len(feature_names)})")
