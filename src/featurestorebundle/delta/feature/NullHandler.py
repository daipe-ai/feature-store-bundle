import math
import numbers
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList


class NullHandler:
    def fill_nulls(self, df: DataFrame, feature_list: FeatureList) -> DataFrame:
        self.__check_fillna_values_valid(feature_list)

        fill_dict = {
            feature.name: feature.template.fillna_value for feature in feature_list.get_all() if feature.template.fillna_value is not None
        }

        return df.fillna(fill_dict)

    def to_storage_format(self, df: DataFrame, feature_list: FeatureList, entity: Entity) -> DataFrame:
        features = [col for col in df.columns if col not in entity.get_primary_key()]

        for feature in features:
            feature_instance = feature_list.get_by_name(feature)

            if feature_instance.template.fillna_value is None:
                df = df.withColumn(feature, f.create_map(f.lit(0), f.col(feature)).cast(feature_instance.storage_dtype))

        return df

    def from_storage_format(self, df: DataFrame, feature_list: FeatureList, entity: Entity) -> DataFrame:
        features = [col for col in df.columns if col not in entity.get_primary_key()]

        for feature in features:
            feature_instance = feature_list.get_by_name(feature)

            if feature_instance.template.fillna_value is None:
                df = df.withColumn(feature, f.col(feature).getItem(0))

        return df

    def __check_fillna_values_valid(self, feature_list: FeatureList):
        for feature in feature_list.get_all():
            if isinstance(feature.template.fillna_value, numbers.Real) and (
                math.isnan(feature.template.fillna_value) or math.isinf(feature.template.fillna_value)
            ):
                raise Exception(f"Invalid fillna value, '{feature.template.fillna_value}' is not supported")
