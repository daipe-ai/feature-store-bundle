from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.delta.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesValidator import FeaturesValidator
from featurestorebundle.delta.feature.DeltaRainbowTableManager import DeltaRainbowTableManager
from featurestorebundle.delta.feature.writer.DeltaFeaturesDataHandler import DeltaFeaturesDataHandler
from featurestorebundle.delta.feature.writer.DeltaFeaturesMergeConfigGenerator import DeltaFeaturesMergeConfigGenerator
from featurestorebundle.delta.feature.writer.DeltaPathFeaturesPreparer import DeltaPathFeaturesPreparer
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface
from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface
from featurestorebundle.metadata.writer.MetadataWriterInterface import MetadataWriterInterface


# pylint: disable=too-many-instance-attributes
class DeltaPathFeaturesWriter(FeaturesWriterInterface):
    def __init__(
        self,
        features_reader: FeaturesReaderInterface,
        metadata_writer: MetadataWriterInterface,
        delta_data_handler: DeltaFeaturesDataHandler,
        delta_merge_config_generator: DeltaFeaturesMergeConfigGenerator,
        features_path_preparer: DeltaPathFeaturesPreparer,
        rainbow_table_manager: DeltaRainbowTableManager,
        features_preparer: FeaturesPreparer,
        features_validator: FeaturesValidator,
        table_names: TableNames,
    ):
        self.__features_reader = features_reader
        self.__metadata_writer = metadata_writer
        self.__delta_data_handler = delta_data_handler
        self.__delta_merge_config_generator = delta_merge_config_generator
        self.__features_path_preparer = features_path_preparer
        self.__rainbow_table_manager = rainbow_table_manager
        self.__features_preparer = features_preparer
        self.__features_validator = features_validator
        self.__table_names = table_names

    def write(self, features_storage: FeaturesStorage):
        entity = features_storage.entity
        feature_list = features_storage.feature_list
        path = self.__table_names.get_features_path(entity.name)
        pk_columns = [entity.id_column, entity.time_column]

        feature_store = self.__features_reader.read_safe(entity.name)
        rainbow_table = self.__rainbow_table_manager.read_safe(entity.name)
        features_data, rainbow_data = self.__features_preparer.prepare(features_storage, feature_store, rainbow_table)
        delta_merge_config = self.__delta_merge_config_generator.generate(entity, features_data, pk_columns)

        self.__features_validator.validate(entity, features_data, feature_list)
        self.__features_path_preparer.prepare(path, entity, feature_list)
        self.__rainbow_table_manager.merge(entity.name, rainbow_data)
        self.__delta_data_handler.merge_to_delta_path(path, delta_merge_config)
        self.__metadata_writer.write(entity, feature_list)
