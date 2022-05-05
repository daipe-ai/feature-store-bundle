import unittest
import datetime as dt
from pyspark.sql import types as t
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.filter.FeaturesFilteringManager import FeaturesFilteringManager
from pysparkbundle.test.PySparkTestCase import PySparkTestCase


class FeaturesFilteringManagerLatestTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )

        self.__container = bootstrapped_container.init("test")
        self.__filtering_manager: FeaturesFilteringManager = self.__container.get(FeaturesFilteringManager)

    def test_simple(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name,
                    "f1",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f1", "", "", "str", "", "", dt.datetime(2020, 1, 1), "daily", dt.datetime(2020, 1, 1)),
                ),
                FeatureInstance(
                    self.__entity.name,
                    "f2",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f2", "", "", "str", "", "", dt.datetime(2020, 1, 1), "daily", dt.datetime(2020, 1, 1)),
                ),
            ]
        )

        features_data = self.__filtering_manager.get_latest(
            feature_store, feature_list, dt.datetime(2020, 1, 1), dt.timedelta(days=1), ["f1", "f2"], False
        )

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", "c1f1", "c1f2"],
                ["2", "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, "f1", "f2"],
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])

    def test_non_existent_feature_raises_exception(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name,
                    "f1",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f1", "", "", "str", "", "", dt.datetime(2020, 1, 1), "daily", dt.datetime(2020, 1, 1)),
                ),
                FeatureInstance(
                    self.__entity.name,
                    "f2",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f2", "", "", "str", "", "", dt.datetime(2020, 1, 1), "daily", dt.datetime(2020, 1, 1)),
                ),
            ]
        )

        with self.assertRaises(Exception):
            self.__filtering_manager.get_latest(feature_store, feature_list, dt.datetime(2020, 1, 1), dt.timedelta(days=1), ["f3"], False)

    def test_more_frequencies(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 8), "c1f1x", "c1f2x", "c1f3"],
                ["2", dt.datetime(2020, 1, 8), "c2f1x", "c2f2x", "c2f3"],
                ["1", dt.datetime(2020, 1, 9), "c1f1y", "c1f2y", None],
                ["2", dt.datetime(2020, 1, 9), "c2f1y", "c2f2y", None],
                ["1", dt.datetime(2020, 1, 10), "c1f1", "c1f2", None],
                ["2", dt.datetime(2020, 1, 10), "c2f1", "c2f2", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        feature_list = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name,
                    "f1",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f1", "", "", "str", "", "", dt.datetime(2020, 1, 1), "1d", dt.datetime(2020, 1, 1)),
                ),
                FeatureInstance(
                    self.__entity.name,
                    "f2",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f2", "", "", "str", "", "", dt.datetime(2020, 1, 1), "1d", dt.datetime(2020, 1, 1)),
                ),
                FeatureInstance(
                    self.__entity.name,
                    "f3",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f3", "", "", "str", "", "", dt.datetime(2020, 1, 1), "7d", dt.datetime(2020, 1, 1)),
                ),
            ]
        )

        features_data = self.__filtering_manager.get_latest(
            feature_store, feature_list, dt.datetime(2020, 1, 10), dt.timedelta(days=10), ["f1", "f2", "f3"], False
        )

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", "c1f1", "c1f2", "c1f3"],
                ["2", "c2f1", "c2f2", "c2f3"],
            ],
            [self.__entity.id_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])

    def test_lookback(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1x", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1x", "c2f2"],
                ["1", dt.datetime(2020, 1, 29), "c1f1y", None],
                ["2", dt.datetime(2020, 1, 29), "c2f1y", None],
                ["1", dt.datetime(2020, 1, 30), "c1f1", None],
                ["2", dt.datetime(2020, 1, 30), "c2f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name,
                    "f1",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f1", "", "", "str", "", "", dt.datetime(2020, 1, 1), "daily", dt.datetime(2020, 1, 1)),
                ),
                FeatureInstance(
                    self.__entity.name,
                    "f2",
                    "",
                    "string",
                    {},
                    FeatureTemplate("f2", "", "", "str", "", "", dt.datetime(2020, 1, 1), "monthly", dt.datetime(2020, 1, 1)),
                ),
            ]
        )

        # lookback 30 days
        features_data = self.__filtering_manager.get_latest(
            feature_store, feature_list, dt.datetime(2020, 1, 30), dt.timedelta(days=30), ["f1", "f2"], True
        )

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", "c1f1", "c1f2"],
                ["2", "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, "f1", "f2"],
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])

        # lookback 15 days
        features_data = self.__filtering_manager.get_latest(
            feature_store, feature_list, dt.datetime(2020, 1, 30), dt.timedelta(days=15), ["f1", "f2"], True
        )

        expected_features_data = self.spark.createDataFrame([], f"{self.__entity.id_column} string, f1 string, f2 string")

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])


if __name__ == "__main__":
    unittest.main()
