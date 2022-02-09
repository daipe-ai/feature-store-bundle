from typing import List
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from featurestorebundle.delta.feature.schema import get_rainbow_table_hash_column, get_rainbow_table_features_column


class FeaturesFilteringManager:
    def get_latest(
        self,
        feature_store: DataFrame,
        rainbow_table: DataFrame,
        features: List[str],
    ):
        if not features:
            features = self.__get_registered_features(feature_store)

        self.__check_features_exist(feature_store, features)

        id_column = feature_store.columns[0]
        time_column = feature_store.columns[1]

        features_data = feature_store.join(rainbow_table, on=get_rainbow_table_hash_column().name).withColumn(
            "features_intersect", f.array_intersect(get_rainbow_table_features_column().name, f.array(*map(f.lit, features)))
        )

        relevant_features = features_data.select(f.array_distinct(f.flatten(f.collect_set("features_intersect")))).collect()[0][0]

        features_data = (
            features_data.groupBy(id_column)
            .agg(
                *[
                    f.array_max(
                        f.collect_list(f.struct(f.array_contains("features_intersect", feature), f.col(time_column), f.col(feature)))
                    )[feature].alias(feature)
                    for feature in relevant_features
                ],
                f.array_distinct(f.flatten(f.collect_set("features_intersect"))).alias("computed_features"),
            )
            .filter(f.size("computed_features") == len(relevant_features))
            .drop("computed_features")
        )

        return features_data

    def get_for_target(
        self,
        feature_store: DataFrame,
        rainbow_table: DataFrame,
        targets: DataFrame,
        features: List[str],
        skip_incomplete_rows: bool,
    ):
        if not features:
            features = self.__get_registered_features(feature_store)

        self.__check_features_exist(feature_store, features)

        if len(targets.columns) != 2:
            raise Exception("Targets dataframe must have exactly two columns [id, date]")

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

        features_data = feature_store.join(targets, on=join_columns).join(rainbow_table, on=get_rainbow_table_hash_column().name)

        return self.__get_complete_rows(features_data, features, skip_incomplete_rows).select(*join_columns, *features)

    def __get_complete_rows(self, features_data: DataFrame, features: List[str], skip_incomplete_rows: bool):
        features_data = features_data.withColumn(
            "features_intersect", f.array_intersect(get_rainbow_table_features_column().name, f.array(*map(f.lit, features)))
        ).withColumn("is_complete_row", f.size("features_intersect") == len(features))

        if skip_incomplete_rows:
            return features_data.filter(f.col("is_complete_row"))

        has_incomplete_rows = len(features_data.filter(~f.col("is_complete_row")).limit(1).collect()) == 1

        if has_incomplete_rows:
            raise Exception("Features contain incomplete rows")

        return features_data

    def __check_features_exist(self, feature_store: DataFrame, features: List[str]):
        registered_features_list = self.__get_registered_features(feature_store)
        unregistered_features = set(features) - set(registered_features_list)

        if unregistered_features != set():
            raise Exception(f"Features {','.join(unregistered_features)} not registered")

    def __get_registered_features(self, feature_store: DataFrame):  # noqa
        return list(feature_store.columns[3:])
