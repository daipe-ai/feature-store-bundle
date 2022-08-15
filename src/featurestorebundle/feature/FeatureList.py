from __future__ import annotations

import re
from typing import List, Dict, Union, Callable
from datetime import datetime

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.utils.errors import UnsupportedChangeFeatureNameError
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.MasterFeature import MasterFeature


class FeatureList:
    def __init__(self, entity: Entity, features: List[FeatureInstance]):
        self.__entity = entity
        self.__features = features

    @property
    def entity(self) -> Entity:
        return self.__entity

    def get_all(self) -> List[FeatureInstance]:
        return self.__features

    def empty(self) -> bool:
        return not self.__features

    def get_names(self) -> List[str]:
        return [feature.name for feature in self.__features]

    def get_by_name(self, feature_name: str) -> FeatureInstance:
        for feature in self.__features:
            if feature.name == feature_name:
                return feature

        raise Exception(f"Cannot find feature {feature_name}")

    def contains_feature(self, feature_name: str) -> bool:
        return feature_name in self.get_names()

    def get_unregistered(self, registered_feature_names: List[str]) -> FeatureList:
        return FeatureList(self.entity, [feature for feature in self.get_all() if feature.name not in registered_feature_names])

    def merge(self, new_feature_list: FeatureList) -> FeatureList:
        return FeatureList(self.entity, self.__features + new_feature_list.get_all())

    def filter(self, condition: Callable) -> FeatureList:
        return FeatureList(self.entity, list(filter(condition, self.__features)))

    def get_metadata(self) -> List[List[Union[Dict[str, str], str]]]:
        return [feature.get_metadata_list() for feature in self.__features]

    def get_metadata_dicts(self) -> List[Dict[str, Union[str, Dict[str, str]]]]:
        return [feature.get_metadata_dict() for feature in self.__features]

    def get_max_last_compute_date(self) -> datetime:
        return max([feature.template.last_compute_date for feature in self.__features])

    def get_change_features(self) -> List[MasterFeature]:
        change_features = list(filter(lambda feature: feature.is_change_feature(), self.__features))
        pattern = re.compile("(?P<first_part_name>.*)_(?P<time_window>[0-9]+[hdw])(?P<second_part_name>.*)")

        result = {}
        for change_feature in change_features:
            match = pattern.fullmatch(change_feature.name)
            if not match:
                raise UnsupportedChangeFeatureNameError(
                    f"Feature with change {change_feature.name} is not in the expected format '.*_[0-9]+[hdw].*'. "
                    f"Either change the name or remove FeatureWithChange classifier."
                )

            first_part_name = match.group("first_part_name")
            second_part_name = match.group("second_part_name")
            time_window = match.group("time_window")
            name = first_part_name + "_{time_window}" + second_part_name

            if name not in result:
                result[name] = ([change_feature], [time_window])
            else:
                features, time_windows = result[name]
                result[name] = (features + [change_feature], time_windows + [time_window])

        return [MasterFeature(name, features, time_windows) for name, (features, time_windows) in result.items()]
