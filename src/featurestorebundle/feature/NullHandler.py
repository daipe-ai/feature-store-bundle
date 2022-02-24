from pyspark.sql import DataFrame

from featurestorebundle.feature.FeatureList import FeatureList


class NullHandler:
    def handle_nulls(self, df: DataFrame, feature_list: FeatureList) -> DataFrame:
        fill_dict = {
            feature.name: feature.template.default_value for feature in feature_list.get_all() if feature.template.default_value is not None
        }
        return df.fillna(fill_dict)
