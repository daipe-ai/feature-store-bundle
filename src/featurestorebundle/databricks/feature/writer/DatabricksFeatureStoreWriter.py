from featurestorebundle.databricks.feature.writer.DatabricksFeatureStoreMergeConfig import DatabricksFeatureStoreMergeConfig
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.FeaturesValidator import FeaturesValidator
from featurestorebundle.delta.feature.DeltaRainbowTableManager import DeltaRainbowTableManager
from featurestorebundle.databricks.feature.writer.DatabricksFeatureStoreDataHandler import DatabricksFeatureStoreDataHandler
from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface
from featurestorebundle.metadata.writer.MetadataWriterInterface import MetadataWriterInterface


# pylint: disable=too-many-instance-attributes
class DatabricksFeatureStoreWriter(FeaturesWriterInterface):
    def __init__(
        self,
        metadata_writer: MetadataWriterInterface,
        databricks_data_handler: DatabricksFeatureStoreDataHandler,
        rainbow_table_manager: DeltaRainbowTableManager,
        features_preparer: FeaturesPreparer,
        features_validator: FeaturesValidator,
        table_names: TableNames,
    ):
        self.__metadata_writer = metadata_writer
        self.__databricks_data_handler = databricks_data_handler
        self.__rainbow_table_manager = rainbow_table_manager
        self.__features_preparer = features_preparer
        self.__features_validator = features_validator
        self.__table_names = table_names

    def write(self, features_storage: FeaturesStorage):
        entity = features_storage.entity
        feature_list = features_storage.feature_list
        full_table_name = self.__table_names.get_features_full_table_name(entity.name)

        write_config = self.__features_preparer.prepare(features_storage)
        databricks_merge_config = DatabricksFeatureStoreMergeConfig(write_config.features_data, entity.get_primary_key())

        self.__features_validator.validate(entity, write_config.features_data, feature_list)

        self.__rainbow_table_manager.merge(entity.name, write_config.rainbow_data)
        self.__databricks_data_handler.merge_to_databricks_feature_store(full_table_name, databricks_merge_config)
        self.__metadata_writer.write(entity, feature_list)
