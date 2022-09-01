import os
from typing import List
from featurestorebundle.feature.deleter.FeaturesDeleterInterface import FeaturesDeleterInterface


class FeaturesDeleter:
    def __init__(self, features_deleter: FeaturesDeleterInterface):
        self.__features_deleter = features_deleter

    def delete(self, features: List[str]):
        dbr_version = tuple(map(int, os.getenv("DATABRICKS_RUNTIME_VERSION").split(".")))  # pyre-ignore[16]

        if dbr_version < (11, 0):
            raise Exception("Feature delete works from DBR 11.0+")

        self.__features_deleter.delete(features)
