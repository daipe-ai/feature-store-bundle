from typing import List
from pyspark.sql import DataFrame


class FeaturesManager:
    def get_registered_features(self, feature_store: DataFrame) -> List[str]:
        return feature_store.columns[2:]

    def get_unregistered_features(self, feature_store: DataFrame, features: List[str]) -> List[str]:
        registered_features_list = self.get_registered_features(feature_store)
        unregistered_features = set(features) - set(registered_features_list)

        return list(unregistered_features)

    def check_features_registered(self, feature_store: DataFrame, features: List[str]):
        unregistered_features = self.get_unregistered_features(feature_store, features)

        if len(unregistered_features) > 0:
            raise Exception(f"Features {','.join(unregistered_features)} not registered")
