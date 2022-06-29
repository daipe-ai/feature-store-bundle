from pyspark.sql import DataFrame
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.filter.IncompleteRowsHandler import IncompleteRowsHandler


class TargetFeaturesFilterer:
    def __init__(self, incomplete_rows_handler: IncompleteRowsHandler):
        self.__incomplete_rows_handler = incomplete_rows_handler

    def get_for_target(
        self,
        feature_store: DataFrame,
        targets: DataFrame,
        feature_list: FeatureList,
        skip_incomplete_rows: bool,
    ) -> DataFrame:
        if len(targets.columns) != 2:
            raise Exception("Targets dataframe must have exactly two columns [id, timestamp]")

        features_id_column = feature_store.schema[0]
        features_time_column = feature_store.schema[1]
        targets_id_column = targets.schema[0]
        targets_time_column = targets.schema[1]
        join_columns = [features_id_column.name, features_time_column.name]

        if features_id_column.name != targets_id_column.name or features_id_column.dataType != targets_id_column.dataType:
            raise Exception(
                f"Features and Targets id column mismatch "
                f"{features_id_column.name} ({features_id_column.dataType.typeName()}) != "
                f"{targets_id_column.name} ({targets_id_column.dataType.typeName()})"
            )

        if features_time_column.name != targets_time_column.name or features_time_column.dataType != targets_time_column.dataType:
            raise Exception(
                f"Features and Targets time column mismatch "
                f"{features_time_column.name} ({features_time_column.dataType.typeName()}) != "
                f"{targets_time_column.name} ({targets_time_column.dataType.typeName()})"
            )

        features_data = feature_store.join(targets, on=join_columns)
        features_data = features_data.select(*join_columns, *feature_list.get_names())
        features_data = self.__incomplete_rows_handler.handle(features_data, skip_incomplete_rows)

        return features_data
