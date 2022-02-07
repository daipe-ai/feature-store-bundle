from typing import List, Set

from pyspark.sql import DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturePattern import FeaturePattern
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate

from featurestorebundle.windows.windowed_features import PERIODS


class TemplateMatchingError(Exception):
    pass


class FeatureTemplateMatcher:
    def get_features(self, entity: Entity, feature_templates: List[FeatureTemplate], df: DataFrame) -> FeatureList:
        pk_columns = [entity.id_column, entity.time_column]

        feature_columns = [col for col in df.schema.jsonValue()["fields"] if col["name"] not in pk_columns]
        feature_patterns = [FeaturePattern(feature_template) for feature_template in feature_templates]
        unmatched_patterns = set(feature_patterns)

        features = [self.__get_feature(col["name"], col["type"], feature_patterns, unmatched_patterns) for col in feature_columns]

        if unmatched_patterns:
            patterns = ", ".join(f'"{pattern.feature_template}"' for pattern in unmatched_patterns)
            raise TemplateMatchingError(f"Templates {patterns} did not match any columns.")

        return FeatureList(features)

    def __get_feature(
        self, name: str, dtype: str, feature_patterns: List[FeaturePattern], unmatched_patterns: Set[FeaturePattern]
    ) -> Feature:
        for feature_pattern in feature_patterns:
            feature_template = feature_pattern.feature_template
            match = feature_pattern.get_match(name)

            if not match:
                continue

            unmatched_patterns.discard(feature_pattern)
            metadata = feature_pattern.get_groups_as_dict(match)

            time_window = metadata.get("time_window", None)

            if time_window is not None:
                self.__check_time_window(time_window, name)

            return Feature.from_template(feature_template, name, dtype, metadata)

        raise TemplateMatchingError(f"Column '{name}' could not be matched by any template.")

    def __check_time_window(self, time_window: str, feature_name: str):
        if not time_window[:-1].isdigit():
            raise Exception(f"In feature '{feature_name}', time_window={time_window[:-1]} is not a positive integer.")
        if not time_window[-1] in PERIODS:
            raise Exception(f"In feature '{feature_name}', time_window period '{time_window[-1]}' is not from {', '.join(PERIODS.keys())}")
