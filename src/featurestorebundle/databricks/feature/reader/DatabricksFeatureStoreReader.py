from logging import Logger
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface
from featurestorebundle.delta.feature.schema import get_feature_store_initial_schema


class DatabricksFeatureStoreReader(FeaturesReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        table_existence_checker: TableExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
        entity_getter: EntityGetter,
    ):
        self.__logger = logger
        self.__table_names = table_names
        self.__table_existence_checker = table_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator
        self.__entity_getter = entity_getter

    def read(self, entity_name: str) -> DataFrame:
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)

        self.__logger.info(f"Reading features from Databricks Feature Store table {full_table_name}")

        return self.__read_from_databricks_feature_store(full_table_name)

    def read_safe(self, entity_name: str) -> DataFrame:
        entity = self.__entity_getter.get_by_name(entity_name)
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)

        if not self.exists(entity_name):
            return self.__empty_dataframe_creator.create(get_feature_store_initial_schema(entity))

        return self.__read_from_databricks_feature_store(full_table_name)

    def exists(self, entity_name: str) -> bool:
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)

        return self.__table_existence_checker.exists(full_table_name)

    def __read_from_databricks_feature_store(self, full_table_name: str) -> DataFrame:
        try:
            from databricks import feature_store  # pyre-ignore[21] pylint: disable=import-outside-toplevel

        except ImportError as exception:
            raise Exception("Cannot import Databricks Feature Store, you need to use ML cluster with DBR 8.3+") from exception

        dbx_feature_store_client = feature_store.FeatureStoreClient()

        return dbx_feature_store_client.read_table(full_table_name)
