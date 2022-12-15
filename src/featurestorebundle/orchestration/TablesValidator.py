from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.delta.PathExistenceChecker import PathExistenceChecker
from featurestorebundle.databricks.DatabricksFeatureStoreClientFactory import DatabricksFeatureStoreClientFactory


class TablesValidator:
    def __init__(
        self,
        spark: SparkSession,
        table_names: TableNames,
        table_existence_checker: TableExistenceChecker,
        path_existence_checker: PathExistenceChecker,
        feature_store_client_factory: DatabricksFeatureStoreClientFactory,
    ):
        self.__spark = spark
        self.__table_names = table_names
        self.__table_existence_checker = table_existence_checker
        self.__path_existence_checker = path_existence_checker
        self.__feature_store_client = feature_store_client_factory.create()

    def validate(self, entity_name: str):
        self.__validate_features_table(entity_name)
        self.__validate_metadata_table(entity_name)

    def __validate_features_table(self, entity_name: str):
        full_table_name = self.__table_names.get_features_full_table_name(entity_name)
        path = self.__table_names.get_features_path(entity_name)

        self.__validate_feature_store_and_hive(full_table_name)
        self.__validate_hive_and_path(full_table_name, path)

    def __validate_metadata_table(self, entity_name: str):
        full_table_name = self.__table_names.get_metadata_full_table_name(entity_name)
        path = self.__table_names.get_metadata_path(entity_name)

        self.__validate_hive_and_path(full_table_name, path)

    def __validate_feature_store_and_hive(self, full_table_name: str):
        if self.__table_exists_in_feature_store(full_table_name) and not self.__table_exists_in_hive(full_table_name):
            raise Exception(f"Table '{full_table_name}' exists in Databricks Feature Store but not in hive")

    def __validate_hive_and_path(self, full_table_name: str, path: str):
        if self.__table_exists_in_hive(full_table_name) and not self.__table_exists_in_path(path):
            raise Exception(f"Table '{full_table_name}' exists in hive but not in path '{path}'")

        if self.__table_exists_in_hive(full_table_name) and self.__get_table_path(full_table_name) != path:
            raise Exception(
                f"Table '{full_table_name}' path '{self.__get_table_path(full_table_name)}' mismatch with path '{path}' specified in config"
            )

    def __table_exists_in_feature_store(self, full_table_name: str) -> bool:
        try:
            self.__feature_store_client.get_table(full_table_name)
            return True

        except Exception:  # noqa pylint: disable=broad-except
            return False

    def __table_exists_in_hive(self, full_table_name: str) -> bool:
        return self.__table_existence_checker.exists(full_table_name)

    def __table_exists_in_path(self, path: str) -> bool:
        return self.__path_existence_checker.exists(path)

    def __get_table_path(self, full_table_name: str) -> str:
        return self.__spark.sql(f"DESCRIBE FORMATTED {full_table_name}").filter(f.col("col_name") == "Location").collect()[0][1]
