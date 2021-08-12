from typing import List, Dict

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.metadata.FeaturePattern import FeaturePattern

PERIODS = {
    "h": "hours",
    "d": "days",
    "w": "weeks",
}


class MetadataExtractor:
    def get_metadata(self, entity: Entity, feature_list: FeatureList, columns: List[str]):
        metadata = []

        feature_columns = self.__get_only_feature_columns(columns, [entity.id_column, entity.time_column])
        feature_patterns = [FeaturePattern(feature) for feature in feature_list.get_all()]

        while feature_columns:
            column_name = feature_columns.pop(0)
            processed = False

            for feature_pattern in feature_patterns:
                feature = feature_pattern.feature
                match = feature_pattern.pattern.match(column_name)

                if not match:
                    continue

                metadata_dict = {placeholder: match.group(placeholder) for placeholder in feature_pattern.placeholders}

                if self.__metadata_values_invalid(metadata_dict):
                    continue

                if "time_window" in metadata_dict:
                    time_window = metadata_dict["time_window"]
                    self.__check_validity_of_time_window(time_window, column_name)

                description = feature.description.format(
                    **{key: (self.__time_window_to_text(val) if key == "time_window" else val) for key, val in metadata_dict.items()}
                )

                metadata.append([column_name, description, feature.category, feature.name, metadata_dict])
                processed = True
            if not processed:
                Exception(f"Column '{column_name}' could not be matched by any template.")

        return metadata

    def __metadata_values_invalid(self, metadata_dict: Dict):
        return any(("_" in val for val in metadata_dict.values()))

    def __get_only_feature_columns(self, columns: List[str], columns_to_remove: List[str]):
        return [col for col in columns if col not in columns_to_remove]

    def __check_validity_of_time_window(self, time_window: str, feature_name: str):
        if not time_window[:-1].isdigit():
            raise Exception(f"In feature '{feature_name}', time_window={time_window[:-1]} is not a positive integer.")
        elif not time_window[-1] in PERIODS.keys():
            raise Exception(f"In feature '{feature_name}', time_window period '{time_window[-1]}' is not from {', '.join(PERIODS.keys())}")

    def __time_window_to_text(self, time_window: str):
        n = int(time_window[:-1])
        period = time_window[-1]

        result = f"{n} {PERIODS[period]}"
        return result[:-1] if n == 1 else result
