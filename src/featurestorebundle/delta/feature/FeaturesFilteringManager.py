from typing import List
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.NullHandler import NullHandler


class FeaturesFilteringManager:
    def __init__(self, null_handler: NullHandler):
        self.__null_handler = null_handler

    def get_latest(
        self,
        feature_store: DataFrame,
        feature_list: FeatureList,
        features: List[str],
        skip_incomplete_rows: bool,
    ):
        if not features:
            features = self.__get_registered_features(feature_store)

        self.__check_features_exist(feature_store, features)

        id_column = feature_store.columns[0]
        time_column = feature_store.columns[1]

        window = (
            Window().partitionBy(id_column).orderBy(f.desc(time_column)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        features_data = (
            feature_store.select(id_column, *[f.first(feature, ignorenulls=True).over(window).alias(feature) for feature in features])
            .groupBy(id_column)
            .agg(*[f.first(feature).alias(feature) for feature in features])
        )

        features_data = self.__get_complete_rows(features_data, skip_incomplete_rows).select(id_column, *features)

        return self.__null_handler.from_storage_format(features_data, feature_list)

    def get_for_target(
        self,
        feature_store: DataFrame,
        targets: DataFrame,
        feature_list: FeatureList,
        features: List[str],
        skip_incomplete_rows: bool,
    ):
        if not features:
            features = self.__get_registered_features(feature_store)

        self.__check_features_exist(feature_store, features)

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
        features_data = self.__get_complete_rows(features_data, skip_incomplete_rows).select(*join_columns, *features)

        return self.__null_handler.from_storage_format(features_data, feature_list)

    def __get_complete_rows(self, features_data: DataFrame, skip_incomplete_rows: bool):
        if skip_incomplete_rows:
            return features_data.na.drop(how="any")

        has_incomplete_rows = (
            len(features_data.filter(f.greatest(*[f.col(i).isNull() for i in features_data.columns])).limit(1).collect()) == 1
        )

        if has_incomplete_rows:
            raise Exception("Features contain incomplete rows")

        return features_data

    def __check_features_exist(self, feature_store: DataFrame, features: List[str]):
        registered_features_list = self.__get_registered_features(feature_store)
        unregistered_features = set(features) - set(registered_features_list)

        if unregistered_features != set():
            raise Exception(f"Features {','.join(unregistered_features)} not registered")

    def __get_registered_features(self, feature_store: DataFrame):  # noqa
        return list(feature_store.columns[2:])
