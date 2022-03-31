import os
import unittest
import datetime as dt

from daipecore.decorator.notebook_function import notebook_function
from pyspark.sql import functions as f, types as t

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.utils.errors import UnsupportedChangeFeatureNameError
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChange import FeatureWithChange
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate
from featurestorebundle.feature.MasterFeature import MasterFeature
from featurestorebundle.notebook.WindowedDataFrame import WindowedDataFrame
from featurestorebundle.notebook.decorator import feature_decorator_factory
from featurestorebundle.notebook.functions.time_windows import sum_windowed, count_windowed
from featurestorebundle.test.PySparkTestCase import PySparkTestCase

os.environ["APP_ENV"] = "test"


class FeatureChangesTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )
        self.__feature_decorator = feature_decorator_factory.create(self.__entity)

    def test_simple(self):
        feature_with_change_template = FeatureWithChangeTemplate("feature_{time_window}", "feature in {time_window}", 0, "int")
        template = FeatureTemplate("a_{time_window}", "a in {time_window}", 0, "int")

        features_with_change = [
            FeatureInstance("entity", "feature_14d", "feature in 14 days", "int", {"time_window": "14d"}, feature_with_change_template),
            FeatureInstance("entity", "feature_30d", "feature in 30 days", "int", {"time_window": "30d"}, feature_with_change_template),
        ]

        feature_list = FeatureList(
            [
                *features_with_change,
                FeatureInstance("entity", "a_14d", "a in 14 days", "int", {"time_window": "14d"}, template),
            ]
        )

        change_features = feature_list.get_change_features()

        self.assertListEqual([MasterFeature("feature_{time_window}", features_with_change, ["14d", "30d"])], change_features)

    def test_time_window_in_the_middle(self):
        feature_with_change_template = FeatureWithChangeTemplate(
            "feature_{time_window}_suffix", "feature suffix in {time_window}", 0, "int"
        )
        template = FeatureTemplate("a_{time_window}", "a in {time_window}", 0, "int")

        features_with_change = [
            FeatureInstance(
                "entity", "feature_14d_suffix", "feature suffix in 14 days", "int", {"time_window": "14d"}, feature_with_change_template
            ),
            FeatureInstance(
                "entity", "feature_30d_suffix", "feature suffix in 30 days", "int", {"time_window": "30d"}, feature_with_change_template
            ),
        ]

        feature_list = FeatureList(
            [
                *features_with_change,
                FeatureInstance("entity", "a_14d", "a in 14 days", "int", {"time_window": "14d"}, template),
            ]
        )

        change_features = feature_list.get_change_features()

        self.assertListEqual([MasterFeature("feature_{time_window}_suffix", features_with_change, ["14d", "30d"])], change_features)

    def test_wrong_name(self):
        feature_with_change_template = FeatureWithChangeTemplate("{time_window}_suffix", "feature suffix in {time_window}", 0, "int")
        template = FeatureTemplate("a_{time_window}", "a in {time_window}", 0, "int")

        features_with_change = [
            FeatureInstance(
                "entity", "14d_suffix", "feature suffix in 14 days", "int", {"time_window": "14d"}, feature_with_change_template
            ),
            FeatureInstance(
                "entity", "30d_suffix", "feature suffix in 30 days", "int", {"time_window": "30d"}, feature_with_change_template
            ),
        ]

        feature_list = FeatureList(
            [
                *features_with_change,
                FeatureInstance("entity", "a_14d", "a in 14 days", "int", {"time_window": "14d"}, template),
            ]
        )

        self.assertRaises(UnsupportedChangeFeatureNameError, feature_list.get_change_features)

    def test_changes_values(self):
        @notebook_function()
        @self.__feature_decorator(
            Feature("f1_count_{time_window}", "f1 count description {time_window}", 0),
            FeatureWithChange("f1_sum_{time_window}", "f1 sum description {time_window}", 0),
        )
        def test():
            df = self.spark.createDataFrame(
                [
                    ["1", dt.datetime(2020, 3, 2), dt.datetime(2020, 3, 1), 100],
                    ["1", dt.datetime(2020, 3, 2), dt.datetime(2020, 2, 1), 300],
                    ["1", dt.datetime(2020, 3, 2), dt.datetime(2020, 1, 1), 500],
                ],
                [self.__entity.id_column, self.__entity.time_column, "date", "f1"],
            )
            wdf = WindowedDataFrame(df, self.__entity, "date", ["20d", "40d"])

            def agg_features(time_window: str):
                return [
                    count_windowed(
                        f"f1_count_{time_window}",
                        f.col("f1"),
                    ),
                    sum_windowed(
                        f"f1_sum_{time_window}",
                        f.col("f1"),
                    ),
                ]

            return wdf.time_windowed(agg_features)

        expected_df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 3, 2), 1, 100, 2, 400, 0.5],
            ],
            [
                self.__entity.id_column,
                self.__entity.time_column,
                "f1_count_20d",
                "f1_sum_20d",
                "f1_count_40d",
                "f2_sum_40d",
                "f1_sum_change_20d_40d",
            ],
        )

        self.assertEqual(expected_df.collect(), test.result.collect())


if __name__ == "__main__":
    unittest.main()
