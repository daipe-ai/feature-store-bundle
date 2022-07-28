from featurestorebundle.delta.TableExistenceChecker import TableExistenceChecker
from featurestorebundle.databricks.DatabricksFeatureStoreClientFactory import DatabricksFeatureStoreClientFactory


class DatabricksFeatureStoreExistenceChecker:
    def __init__(
        self,
        table_existence_checker: TableExistenceChecker,
        feature_store_client_factory: DatabricksFeatureStoreClientFactory,
    ):
        self.__table_existence_checker = table_existence_checker
        self.__feature_store_client = feature_store_client_factory.create()

    def exists(self, full_table_name: str) -> bool:
        if self.__table_exists_in_feature_store(full_table_name) and not self.__table_exists_in_hive(full_table_name):
            raise Exception(f"Table {full_table_name} exists in Databricks Feature Store but not in hive")

        if not self.__table_exists_in_feature_store(full_table_name) and self.__table_exists_in_hive(full_table_name):
            raise Exception(f"Table {full_table_name} exists in hive but not in Databricks Feature Store")

        if self.__table_exists_in_feature_store(full_table_name) and self.__table_exists_in_hive(full_table_name):
            return True

        if not self.__table_exists_in_feature_store(full_table_name) and not self.__table_exists_in_hive(full_table_name):
            return False

        raise Exception(f"Databricks Feature store table {full_table_name} is in inconsistent state")

    def __table_exists_in_feature_store(self, full_table_name: str) -> bool:
        try:
            self.__feature_store_client.get_table(full_table_name)
            return True

        except Exception:  # noqa pylint: disable=broad-except
            return False

    def __table_exists_in_hive(self, full_table_name: str) -> bool:
        return self.__table_existence_checker.exists(full_table_name)
