from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.feature.writer.DeltaFeaturesMergeConfigGenerator import DeltaFeaturesMergeConfigGenerator
from featurestorebundle.delta.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.FeaturesValidator import FeaturesValidator
from featurestorebundle.delta.feature.writer.DeltaFeaturesDataHandler import DeltaFeaturesDataHandler
from featurestorebundle.delta.feature.writer.DeltaTableFeaturesPreparer import DeltaTableFeaturesPreparer
from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface
from featurestorebundle.metadata.writer.MetadataWriterInterface import MetadataWriterInterface


# pylint: disable=too-many-instance-attributes
class DeltaTableFeaturesWriter(FeaturesWriterInterface):
    def __init__(
        self,
        metadata_writer: MetadataWriterInterface,
        delta_data_handler: DeltaFeaturesDataHandler,
        features_table_preparer: DeltaTableFeaturesPreparer,
        features_preparer: FeaturesPreparer,
        features_validator: FeaturesValidator,
        table_names: TableNames,
    ):
        self.__metadata_writer = metadata_writer
        self.__delta_data_handler = delta_data_handler
        self.__features_table_preparer = features_table_preparer
        self.__features_preparer = features_preparer
        self.__features_validator = features_validator
        self.__table_names = table_names

    def write(self, features_storage: FeaturesStorage):
        entity = features_storage.entity
        feature_list = features_storage.feature_list
        full_table_name = self.__table_names.get_features_full_table_name(entity.name)
        path = self.__table_names.get_features_path(entity.name)

        features_data = self.__features_preparer.prepare(features_storage)
        merge_config = DeltaFeaturesMergeConfigGenerator().generate(entity, features_data, entity.get_primary_key())

        self.__features_validator.validate(entity, features_data, feature_list)
        self.__features_table_preparer.prepare(full_table_name, path, entity, feature_list)
        self.__delta_data_handler.merge_to_delta_table(full_table_name, merge_config)
        self.__metadata_writer.write(entity, feature_list)
