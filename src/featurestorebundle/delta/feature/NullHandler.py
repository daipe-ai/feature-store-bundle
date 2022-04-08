import math
import numbers
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturesValidator import FeaturesValidator


class NullHandler:
    def __init__(self, features_validator: FeaturesValidator):
        self.__features_validator = features_validator

    def fill_nulls(self, df: DataFrame, feature_list: FeatureList) -> DataFrame:
        self.__check_fillna_values_valid(feature_list)

        fill_dict = {
            feature.name: feature.template.fillna_value for feature in feature_list.get_all() if feature.template.fillna_value is not None
        }

        return df.fillna(fill_dict)

    def to_storage_format(self, df: DataFrame, feature_list: FeatureList, entity: Entity) -> DataFrame:
        self.__features_validator.validate(entity, df, feature_list)

        return df.select(
            *entity.get_primary_key(),
            *[
                f.create_map(f.lit(0), f.col(feature.name)).cast(feature.storage_dtype).alias(feature.name)
                if feature.template.fillna_value is None
                else f.col(feature.name)
                for feature in feature_list.get_all()
            ],
        )

    def from_storage_format(self, df: DataFrame, feature_list: FeatureList, entity: Entity) -> DataFrame:
        self.__features_validator.validate(entity, df, feature_list)

        return df.select(
            *entity.get_primary_key(),
            *[
                f.col(feature.name).getItem(0).alias(feature.name) if feature.template.fillna_value is None else f.col(feature.name)
                for feature in feature_list.get_all()
            ],
        )

    def __check_fillna_values_valid(self, feature_list: FeatureList):
        for feature in feature_list.get_all():
            if isinstance(feature.template.fillna_value, numbers.Real) and (
                math.isnan(feature.template.fillna_value) or math.isinf(feature.template.fillna_value)
            ):
                raise Exception(f"Invalid fillna value, '{feature.template.fillna_value}' is not supported")
