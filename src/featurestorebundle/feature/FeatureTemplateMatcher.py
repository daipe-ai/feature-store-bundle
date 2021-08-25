from typing import List, Set

from pyspark.sql import DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturePattern import FeaturePattern
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.metadata.TimeWindowHandler import TimeWindowHandler


class FeatureTemplateMatcher:
    def __init__(self, time_window_handler: TimeWindowHandler):
        self.__time_window_handler = time_window_handler

    def get_features(self, entity: Entity, feature_templates: List[FeatureTemplate], df: DataFrame) -> FeatureList:
        pk_columns = [entity.id_column, entity.time_column]

        feature_columns = [col for col in df.schema.jsonValue()["fields"] if col["name"] not in pk_columns]
        feature_patterns = [FeaturePattern(feature_template) for feature_template in feature_templates]
        unmatched_patterns = set(feature_patterns)

        features = [self.__get_feature(col["name"], col["type"], feature_patterns, unmatched_patterns) for col in feature_columns]

        if unmatched_patterns:
            patterns = ", ".join(f'"{pattern}"' for pattern in unmatched_patterns)
            raise Exception(f"Templates {patterns} did not match any columns.")

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

            if "time_window" in metadata:
                time_window = metadata["time_window"]
                self.__time_window_handler.is_valid(time_window, name)

            description = feature_template.description_template.format(
                **{
                    key: (self.__time_window_handler.to_text(val) if key == "time_window" else val.replace("_", " "))
                    for key, val in metadata.items()
                }
            )

            return Feature(name, description, dtype, metadata, feature_template)

        raise Exception(f"Column '{name}' could not be matched by any template.")
