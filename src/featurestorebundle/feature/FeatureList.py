from typing import List, Dict, Union

from featurestorebundle.feature.Feature import Feature


class FeatureList:
    def __init__(self, features: List[Feature]):
        self.__features = features

    def get_all(self) -> List[Feature]:
        return self.__features

    def empty(self):
        return not self.__features

    def get_names(self):
        return [feature.name for feature in self.__features]

    def get_unregistered(self, registered_feature_names: List[str]) -> "FeatureList":
        def registered(instance: Feature, registered_names: List[str]):
            return instance.name in registered_names

        return FeatureList([feature for feature in self.get_all() if not registered(feature, registered_feature_names)])

    def merge(self, new_feature_list: "FeatureList") -> "FeatureList":
        return FeatureList(self.__features + new_feature_list.get_all())

    def get_metadata(self) -> List[List[Union[Dict[str, str], str]]]:
        return [feature.get_metadata_list() for feature in self.__features]

    def get_metadata_dicts(self) -> List[Dict[str, Union[str, Dict[str, str]]]]:
        return [feature.get_metadata_dict() for feature in self.__features]
