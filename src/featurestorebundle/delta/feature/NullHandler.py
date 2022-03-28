import numpy as np
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from featurestorebundle.feature.FeatureList import FeatureList


class NullHandler:
    def fill_nulls(self, df: DataFrame, feature_list: FeatureList) -> DataFrame:
        self.__check_default_values_valid(feature_list)

        fill_dict = {
            feature.name: feature.template.default_value for feature in feature_list.get_all() if feature.template.default_value is not None
        }

        return df.fillna(fill_dict)

    def to_storage_format(self, df: DataFrame, feature_list: FeatureList) -> DataFrame:
        return df.select(
            *self.__get_primary_keys(df, feature_list),
            *[
                f.create_map(f.lit(0), f.col(feature.name)).cast(feature.storage_dtype).alias(feature.name)
                if feature.template.default_value is None
                else f.col(feature.name)
                for feature in feature_list.get_all()
            ],
        )

    def from_storage_format(self, df: DataFrame, feature_list: FeatureList) -> DataFrame:
        return df.select(
            *self.__get_primary_keys(df, feature_list),
            *[
                f.col(feature.name).getItem(0).alias(feature.name) if feature.template.default_value is None else f.col(feature.name)
                for feature in feature_list.get_all()
            ],
        )

    def __check_default_values_valid(self, feature_list: FeatureList):
        for feature in feature_list.get_all():
            if feature.template.default_value in [np.nan, np.inf, -np.inf, float("nan"), float("inf"), float("-inf")]:
                raise Exception(f"Invalid default value, '{feature.template.default_value}' is not supported")

    def __get_primary_keys(self, df: DataFrame, feature_list: FeatureList) -> List[str]:
        return list(set(df.columns) - set(feature_list.get_names()))
