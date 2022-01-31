import re
from typing import List, Dict, Union

from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.MasterFeature import MasterFeature


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

    def get_change_features(self) -> List[MasterFeature]:
        change_features = list(filter(lambda feature: feature.is_change_feature(), self.__features))
        pattern = re.compile("(?P<name>.*)_(?P<time_window>[0-9]+[hdw])")

        result = {}
        for change_feature in change_features:
            match = pattern.fullmatch(change_feature.name)
            if not match:
                raise Exception(
                    f"Feature with change {change_feature.name} is not in the expected format 'feature_name_[0-9]+[hdw]'. Either change the name or remove FeatureWithChange classifier."
                )

            name = match.group("name")
            time_window = match.group("time_window")

            if name not in result:
                result[name] = ([change_feature], [time_window])
            else:
                features, time_windows = result[name]
                result[name] = (features + [change_feature], time_windows + [time_window])

        return [MasterFeature(name, features, time_windows) for name, (features, time_windows) in result.items()]
