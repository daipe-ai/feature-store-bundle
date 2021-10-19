from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.TablePreparer import TablePreparer
from featurestorebundle.metadata.MetadataTablePreparer import MetadataTablePreparer
from featurestorebundle.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesValidator import FeaturesValidator
from featurestorebundle.databricks.DatabricksDataHandler import DatabricksDataHandler
from featurestorebundle.databricks.DatabricksMergeConfig import DatabricksMergeConfig
from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface


class DatabricksFeatureStoreWriter(FeaturesWriterInterface):
    def __init__(
        self,
        table_preparer: TablePreparer,
        databricks_data_handler: DatabricksDataHandler,
        features_preparer: FeaturesPreparer,
        metadata_table_preparer: MetadataTablePreparer,
        features_validator: FeaturesValidator,
        table_names: TableNames,
    ):
        self.__databricks_data_handler = databricks_data_handler
        self.__table_preparer = table_preparer
        self.__features_preparer = features_preparer
        self.__metadata_table_preparer = metadata_table_preparer
        self.__features_validator = features_validator
        self.__table_names = table_names

    def write_latest(self, features_storage: FeaturesStorage):
        entity = features_storage.entity
        feature_list = features_storage.feature_list
        features_data = self.prepare_features(features_storage)

        self.__features_validator.validate(entity, features_data, feature_list)

        full_table_name = self.__table_names.get_latest_full_table_name(entity.name)
        metadata_full_table_name = self.__table_names.get_latest_metadata_full_table_name(entity.name)
        metadata_path = self.__table_names.get_latest_metadata_path(entity.name)
        pk_columns = [entity.id_column]

        self.__metadata_table_preparer.prepare(metadata_full_table_name, metadata_path, entity)

        config = DatabricksMergeConfig(features_data, pk_columns)

        self.__databricks_data_handler.write(full_table_name, metadata_full_table_name, config, feature_list)

    def write_historized(self, features_storage: FeaturesStorage):
        entity = features_storage.entity
        feature_list = features_storage.feature_list
        features_data = self.prepare_features(features_storage)

        self.__features_validator.validate(entity, features_data, feature_list)

        full_table_name = self.__table_names.get_historized_full_table_name(entity.name)
        metadata_full_table_name = self.__table_names.get_historized_metadata_full_table_name(entity.name)
        metadata_path = self.__table_names.get_historized_metadata_path(entity.name)
        pk_columns = [entity.id_column, entity.time_column]

        self.__metadata_table_preparer.prepare(metadata_full_table_name, metadata_path, entity)

        config = DatabricksMergeConfig(features_data, pk_columns)

        self.__databricks_data_handler.write(full_table_name, metadata_full_table_name, config, feature_list)

    def prepare_features(self, features_storage: FeaturesStorage) -> DataFrame:
        return self.__features_preparer.prepare(features_storage.entity, features_storage.results)
