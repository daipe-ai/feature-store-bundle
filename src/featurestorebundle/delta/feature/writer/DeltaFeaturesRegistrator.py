from typing import List
from pyspark.sql import SparkSession
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList


class DeltaFeaturesRegistrator:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def register(self, table_identifier: str, feature_list: FeatureList):
        def build_add_column_string(feature: FeatureInstance):
            return f'{feature.name} {feature.storage_dtype} COMMENT "{feature.description}"'

        def build_add_columns_string(table_identifier, feature_list: FeatureList):
            add_column_sqls = [build_add_column_string(feature) for feature in feature_list.get_all()]
            return f"ALTER TABLE {table_identifier} ADD COLUMNS ({','.join(add_column_sqls)})"

        registered_feature_names = self.__get_feature_names(table_identifier)
        unregistered_features = feature_list.get_unregistered(registered_feature_names)

        if not unregistered_features.empty():
            self.__spark.sql(build_add_columns_string(table_identifier, unregistered_features))

    def __get_feature_names(self, table_identifier: str) -> List[str]:
        column_definitions = self.__spark.sql(f"DESCRIBE TABLE {table_identifier}").collect()

        def find_separation_row(column_definitions):
            for i, row in enumerate(column_definitions):
                if row.col_name in ["", "# Partition Information"]:
                    return i

            return None

        feature_rows = column_definitions[2 : find_separation_row(column_definitions)]

        return [row.col_name for row in feature_rows]
