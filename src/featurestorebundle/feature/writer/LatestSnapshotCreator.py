from logging import Logger
from pyspark.sql import functions as f
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory
from featurestorebundle.feature.getter.LatestFeaturesGetter import LatestFeaturesGetter
from featurestorebundle.metadata.reader.MetadataTableReader import MetadataTableReader
from featurestorebundle.databricks.featurestore.DatabricksFeatureStorePreparer import DatabricksFeatureStorePreparer
from featurestorebundle.databricks.featurestore.DatabricksFeatureStoreDataHandler import DatabricksFeatureStoreDataHandler


# pylint: disable=too-many-instance-attributes
class LatestSnapshotCreator:
    def __init__(
        self,
        logger: Logger,
        latest_features_getter: LatestFeaturesGetter,
        metadata_reader: MetadataTableReader,
        feature_list_factory: FeatureListFactory,
        databricks_feature_store_preparer: DatabricksFeatureStorePreparer,
        databricks_feature_store_data_handler: DatabricksFeatureStoreDataHandler,
        table_names: TableNames,
    ):
        self.__logger = logger
        self.__latest_features_getter = latest_features_getter
        self.__metadata_reader = metadata_reader
        self.__feature_list_factory = feature_list_factory
        self.__databricks_feature_store_preparer = databricks_feature_store_preparer
        self.__databricks_feature_store_data_handler = databricks_feature_store_data_handler
        self.__table_names = table_names

    def create(self, entity: Entity):
        base_db = self.__table_names.get_features_base_db_name(entity.name)
        full_table_name = self.__table_names.get_latest_features_full_table_name(entity.name)
        path = self.__table_names.get_latest_features_path(entity.name)
        metadata = self.__metadata_reader.read(entity.name).filter(f.col("entity") == entity.name).filter(f.col("base_db") == base_db)
        feature_list = self.__feature_list_factory.create(entity, metadata)
        timestamp = feature_list.get_max_last_compute_date()

        self.__logger.info(f"Creating latest snapshot '{full_table_name}' for timestamp '{timestamp.strftime('%Y-%m-%d')}'")

        features_data = self.__latest_features_getter.get_latest(feature_list, timestamp)

        self.__databricks_feature_store_preparer.prepare(full_table_name, path, entity)

        self.__databricks_feature_store_data_handler.overwrite(full_table_name, features_data)

        self.__logger.info("Latest snapshot successfully created")
