from featurestorebundle.entity.Entity import Entity
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.writer.FeaturesMergeConfigGenerator import FeaturesMergeConfigGenerator
from featurestorebundle.feature.writer.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.FeaturesValidator import FeaturesValidator
from featurestorebundle.metadata.MetadataValidator import MetadataValidator
from featurestorebundle.feature.writer.FeaturesTableDataHandler import FeaturesTableDataHandler
from featurestorebundle.feature.writer.FeaturesTablePreparer import FeaturesTablePreparer
from featurestorebundle.feature.writer.LatestSnapshotCreator import LatestSnapshotCreator
from featurestorebundle.metadata.writer.MetadataTableWriter import MetadataTableWriter
from featurestorebundle.widgets.WidgetsGetter import WidgetsGetter


# pylint: disable=too-many-instance-attributes
class FeaturesTableWriter:
    def __init__(
        self,
        metadata_writer: MetadataTableWriter,
        features_data_handler: FeaturesTableDataHandler,
        features_table_preparer: FeaturesTablePreparer,
        latest_snapshot_creator: LatestSnapshotCreator,
        features_preparer: FeaturesPreparer,
        features_validator: FeaturesValidator,
        metadata_validator: MetadataValidator,
        widgets_getter: WidgetsGetter,
        table_names: TableNames,
    ):
        self.__metadata_writer = metadata_writer
        self.__features_data_handler = features_data_handler
        self.__features_table_preparer = features_table_preparer
        self.__latest_snapshot_creator = latest_snapshot_creator
        self.__features_preparer = features_preparer
        self.__features_validator = features_validator
        self.__metadata_validator = metadata_validator
        self.__widgets_getter = widgets_getter
        self.__table_names = table_names

    def write(self, features_storage: FeaturesStorage):
        entity = features_storage.entity
        feature_list = features_storage.feature_list
        full_table_name = self.__get_features_table(entity)
        path = self.__get_features_path(entity)

        features_data = self.__features_preparer.prepare(features_storage)
        merge_config = FeaturesMergeConfigGenerator().generate(entity, features_data, entity.get_primary_key())

        self.__features_validator.validate(entity, features_data, feature_list)
        self.__metadata_validator.validate(entity, feature_list)
        self.__features_table_preparer.prepare(full_table_name, path, entity, feature_list)
        self.__features_data_handler.merge(full_table_name, merge_config)
        self.__metadata_writer.write(feature_list)
        self.__create_latest_snapshot(entity)

    def __get_features_table(self, entity: Entity) -> str:
        return (
            self.__table_names.get_archive_features_full_table_name(entity.name, self.__widgets_getter.get_timestamp())
            if self.__widgets_getter.timestamp_exists()
            else self.__table_names.get_target_features_full_table_name(entity.name, self.__widgets_getter.get_target())
        )

    def __get_features_path(self, entity: Entity) -> str:
        return (
            self.__table_names.get_archive_features_path(entity.name, self.__widgets_getter.get_timestamp())
            if self.__widgets_getter.timestamp_exists()
            else self.__table_names.get_target_features_path(entity.name, self.__widgets_getter.get_target())
        )

    def __create_latest_snapshot(self, entity: Entity):
        if self.__widgets_getter.timestamp_exists():
            self.__latest_snapshot_creator.create(entity)
