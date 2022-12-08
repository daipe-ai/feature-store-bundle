from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.FeaturesValidator import FeaturesValidator
from featurestorebundle.metadata.MetadataValidator import MetadataValidator
from featurestorebundle.delta.feature.writer.DeltaFeaturesDataHandler import DeltaFeaturesDataHandler
from featurestorebundle.delta.feature.writer.DeltaFeaturesMergeConfigGenerator import DeltaFeaturesMergeConfigGenerator
from featurestorebundle.delta.feature.writer.DeltaTableFeaturesPreparer import DeltaTableFeaturesPreparer
from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface
from featurestorebundle.metadata.writer.MetadataWriter import MetadataWriter


# pylint: disable=too-many-instance-attributes
class DeltaTableFeaturesWriter(FeaturesWriterInterface):
    def __init__(
        self,
        metadata_writer: MetadataWriter,
        delta_data_handler: DeltaFeaturesDataHandler,
        merge_config_generator: DeltaFeaturesMergeConfigGenerator,
        features_table_preparer: DeltaTableFeaturesPreparer,
        features_preparer: FeaturesPreparer,
        features_validator: FeaturesValidator,
        metadata_validator: MetadataValidator,
        table_names: TableNames,
    ):
        self.__metadata_writer = metadata_writer
        self.__delta_data_handler = delta_data_handler
        self.__merge_config_generator = merge_config_generator
        self.__features_table_preparer = features_table_preparer
        self.__features_preparer = features_preparer
        self.__features_validator = features_validator
        self.__metadata_validator = metadata_validator
        self.__table_names = table_names

    def write(self, features_storage: FeaturesStorage):
        entity = features_storage.entity
        feature_list = features_storage.feature_list
        full_table_name = self.__table_names.get_features_full_table_name(entity.name)
        path = self.__table_names.get_features_path(entity.name)

        features_data = self.__features_preparer.prepare(features_storage)
        merge_config = self.__merge_config_generator.generate(entity, features_data, entity.get_primary_key())

        self.__features_validator.validate(entity, features_data, feature_list)
        self.__metadata_validator.validate(entity, feature_list)
        self.__features_table_preparer.prepare(full_table_name, path, entity, feature_list)
        self.__delta_data_handler.merge_to_delta_table(full_table_name, merge_config)
        self.__metadata_writer.write(feature_list)

    def get_location(self, entity_name: str) -> str:
        return self.__table_names.get_features_full_table_name(entity_name)

    def get_backend(self) -> str:
        return "delta_table"
