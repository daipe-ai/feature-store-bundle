from typing import List
from featurestorebundle.feature.Feature import Feature


class FeatureList:
    def __init__(self, features: List[Feature]):
        self.__features = features

    def get_all(self):
        return self.__features

    def is_empty(self):
        return self.__features == []

    def get_names(self):
        return [feature.name for feature in self.__features]

    def get_feature_by_name(self, feature_name: str) -> Feature:
        for feature in self.__features:
            if feature.name == feature_name:
                return feature

        raise Exception(f"Feature {feature_name} not found")

    def get_unregistered(self, registered_feature_names: list):
        return FeatureList([feature for feature in self.__features if feature.name not in registered_feature_names])

    def merge(self, new_feature_list: "FeatureList"):
        return FeatureList(self.__features + new_feature_list.get_all())
