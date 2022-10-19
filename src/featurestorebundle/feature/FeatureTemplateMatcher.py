from typing import List, Set

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.utils.errors import TemplateMatchingError, TimeWindowFormatError
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturePattern import FeaturePattern
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate

from featurestorebundle.notebook.functions.time_windows import PERIODS
from featurestorebundle.utils.types import get_variable_type_default, normalize_dtype


class FeatureTemplateMatcher:
    def get_features(self, entity: Entity, feature_templates: List[FeatureTemplate], df: DataFrame) -> FeatureList:
        pk_columns = [entity.id_column, entity.time_column]

        feature_patterns = [FeaturePattern(feature_template) for feature_template in feature_templates]
        unmatched_patterns = set(feature_patterns)

        features = [
            self.__get_feature(
                entity.name,
                field.name,
                self.__get_dtype(field),
                self.__get_variable_type(field),
                feature_patterns,
                unmatched_patterns,
            )
            for field in df.schema.fields
            if field.name not in pk_columns
        ]

        if unmatched_patterns:
            patterns = ", ".join(f'"{pattern.feature_template}"' for pattern in unmatched_patterns)
            raise TemplateMatchingError(f"Templates {patterns} did not match any columns.")

        return FeatureList(entity, features)

    def __get_feature(
        self,
        entity: str,
        name: str,
        dtype: str,
        variable_type: str,
        feature_patterns: List[FeaturePattern],
        unmatched_patterns: Set[FeaturePattern],
    ) -> FeatureInstance:
        for feature_pattern in feature_patterns:
            feature_template = feature_pattern.feature_template
            match = feature_pattern.get_match(name)

            if not match:
                continue

            unmatched_patterns.discard(feature_pattern)
            extra = feature_pattern.get_groups_as_dict(match)

            time_window = extra.get("time_window", None)

            if time_window is not None:
                self.__check_time_window(feature_pattern, time_window, name)

            return FeatureInstance.from_template(feature_template, entity, name, dtype, variable_type, extra)

        raise TemplateMatchingError(f"Column '{name}' could not be matched by any template.")

    def __get_dtype(self, field: StructField) -> str:
        return normalize_dtype(field.dataType.simpleString())

    def __get_variable_type(self, field: StructField) -> str:
        return (
            field.metadata["variable_type"]
            if "variable_type" in field.metadata
            else get_variable_type_default(normalize_dtype(field.dataType.simpleString()))
        )

    def __check_time_window(self, feature_pattern: FeaturePattern, time_window: str, feature_name: str):
        time_window_value = time_window[:-1]
        time_window_period = time_window[-1]

        if not time_window_value.isdigit():
            raise TimeWindowFormatError(
                f"Column '{feature_name}' has been matched by '{feature_pattern.feature_template.name_template}' and "
                f"time_window={time_window_value} which is not a positive integer. Check that your templates adhere to the rules at "
                f"https://docs.daipe.ai/feature-store/templates/"
            )
        if time_window_period not in PERIODS:
            raise TimeWindowFormatError(
                f"Column '{feature_name}' has been matched by '{feature_pattern.feature_template.name_template}' and "
                f"time_window={time_window} with period '{time_window_period}' is not from supported periods: {', '.join(PERIODS.keys())}"
            )
