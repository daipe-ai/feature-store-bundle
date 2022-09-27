from typing import List
from pyspark.sql import SparkSession
from featurestorebundle.feature.FeatureList import FeatureList

from featurestorebundle.feature.writer.AddColumnsQueryBuilder import AddColumnsQueryBuilder


class FeaturesTableRegistrator:
    def __init__(self, spark: SparkSession, add_columns_query_builder: AddColumnsQueryBuilder):
        self.__spark = spark
        self.__add_columns_query_builder = add_columns_query_builder

    def register(self, full_table_name: str, feature_list: FeatureList):
        registered_feature_names = self.__get_feature_names(full_table_name)
        unregistered_features = feature_list.get_unregistered(registered_feature_names)

        if not unregistered_features.empty():
            self.__spark.sql(self.__add_columns_query_builder.build_add_columns_query(full_table_name, unregistered_features))

    def __get_feature_names(self, full_table_name: str) -> List[str]:
        column_definitions = self.__spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()

        def find_separation_row(column_definitions):  # noqa
            for i, row in enumerate(column_definitions):
                if row.col_name in ["", "# Partition Information"]:
                    return i

            return None

        feature_rows = column_definitions[2 : find_separation_row(column_definitions)]  # noqa

        return [row.col_name for row in feature_rows]
