import datetime as dt
import os
import unittest

from pyfonycore.bootstrap import bootstrapped_container
from pyspark.sql import types as t
from daipecore.decorator.notebook_function import notebook_function

from featurestorebundle.delta.feature.FeaturesJoiner import FeaturesJoiner
from featurestorebundle.delta.feature.schema import get_feature_store_initial_schema, get_rainbow_table_schema
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureWithChange import FeatureWithChange
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.notebook.decorator import feature_decorator_factory
from featurestorebundle.test.PySparkTestCase import PySparkTestCase

os.environ["APP_ENV"] = "test"


class NullHandlerTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__container = bootstrapped_container.init("test")
        self.__features_joiner: FeaturesJoiner = self.__container.get(FeaturesJoiner)

        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )

    def test_nulls_after_decorator(self):
        feature_decorator = feature_decorator_factory.create(self.__entity)

        @notebook_function()
        @feature_decorator(
            Feature("f1", "f1 description", "default"),
            Feature("f2", "f2 description", None),
        )
        def test():
            return self.spark.createDataFrame(
                [
                    ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                    ["3", dt.date(2020, 1, 1), None, None],
                    ["4", dt.date(2020, 1, 1), "c4f1", None],
                ],
                [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
            )

        expected_df = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                ["3", dt.date(2020, 1, 1), "default", None],
                ["4", dt.date(2020, 1, 1), "c4f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.assertEqual(expected_df.collect(), test.result.collect())

    def test_changes_null_values(self):
        features_storage = FeaturesStorage(self.__entity)
        feature_decorator = feature_decorator_factory.create(self.__entity, features_storage)

        string_default = "test"
        change_feature_default = 3
        change_default = 0.0
        count_default = 5

        @notebook_function()
        @feature_decorator(
            Feature("f1_cat_{time_window}", "f1 cat description {time_window}", string_default),
            FeatureWithChange("f1_sum_{time_window}", "f1 sum description {time_window}", change_feature_default),
        )
        def test1():
            return self.spark.createDataFrame(
                [
                    ["1", dt.date(2020, 3, 2), "hello", 100, "world", 400],
                    ["3", dt.date(2020, 3, 2), "hello", None, "world", 500],
                ],
                [
                    self.__entity.id_column,
                    self.__entity.time_column,
                    "f1_cat_20d",
                    "f1_sum_20d",
                    "f1_cat_40d",
                    "f1_sum_40d",
                ],
            )

        @notebook_function()
        @feature_decorator(
            Feature("f2_count_{time_window}", "f2 description {time_window}", count_default),
        )
        def test2():
            return self.spark.createDataFrame(
                [
                    ["2", dt.date(2020, 3, 2), 1, None],
                ],
                schema=f"{self.__entity.id_column} string, {self.__entity.time_column} date, f2_count_20d long, f2_count_40d long",
            )

        feature_store = self.spark.createDataFrame([], get_feature_store_initial_schema(self.__entity))
        rainbow_table = self.spark.createDataFrame([], get_rainbow_table_schema())

        features_data, _ = self.__features_joiner.join(features_storage, feature_store, rainbow_table)
        features_data = features_data.drop("features_hash")

        expected_df = self.spark.createDataFrame(
            [
                ["3", dt.date(2020, 3, 2), "hello", change_feature_default, "world", 500, change_default, count_default, count_default],
                [
                    "2",
                    dt.date(2020, 3, 2),
                    string_default,
                    change_feature_default,
                    string_default,
                    change_feature_default,
                    change_default,
                    1,
                    count_default,
                ],
                ["1", dt.date(2020, 3, 2), "hello", 100, "world", 400, 0.5, count_default, count_default],
            ],
            [
                self.__entity.id_column,
                self.__entity.time_column,
                "f1_cat_20d",
                "f1_sum_20d",
                "f1_cat_40d",
                "f1_sum_40d",
                "f1_sum_change_20d_40d",
                "f2_count_20d",
                "f2_count_40d",
            ],
        )

        self.compare_dataframes(expected_df, features_data, self.__entity.get_primary_key())


if __name__ == "__main__":
    unittest.main()
