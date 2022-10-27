import os
from databricksbundle.detector import is_databricks


class DatabricksFeatureStoreClientFactory:
    def create(self):
        if not is_databricks():
            return None

        dbr_version = tuple(map(int, os.getenv("DATABRICKS_RUNTIME_VERSION").split(".")))

        if dbr_version < (10, 4):
            raise Exception("Databricks Feature Store may be used from DBR 10.4+")

        try:
            from databricks import feature_store  # pyre-ignore[21] pylint: disable=import-outside-toplevel

        except ImportError as exception:
            raise Exception("Cannot import Databricks Feature Store, you need to use ML cluster with DBR 10.4+") from exception

        return feature_store.FeatureStoreClient()
