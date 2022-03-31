import unittest
import datetime as dt
from pyspark.sql import types as t
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.FeaturesFilteringManager import FeaturesFilteringManager
from featurestorebundle.test.PySparkTestCase import PySparkTestCase


class FeaturesFilteringManagerTargetsTest(PySparkTestCase):
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
                ["2", dt.datetime(2018, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", "EMPTY", "str")
                ),
                FeatureInstance(
                    self.__entity.name, "f2", "this is feature 2", "string", {}, FeatureTemplate("f2", "this is feature 2", "EMPTY", "str")
                ),
            ]
        )

        targets_table = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1)],
                ["2", dt.datetime(2018, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column],
        )

        features_data = self.__filtering_manager.get_for_target(feature_store, targets_table, feature_list, ["f1", "f2"], False)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2018, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column, self.__entity.time_column])

    def test_incomplete_rows_raise_exception(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2018, 1, 1), "c2f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", "EMPTY", "str")
                ),
                FeatureInstance(
                    self.__entity.name, "f2", "this is feature 2", "string", {}, FeatureTemplate("f2", "this is feature 2", "EMPTY", "str")
                ),
            ]
        )

        targets_table = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1)],
                ["2", dt.datetime(2018, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column],
        )

        with self.assertRaisesRegex(Exception, "Features contain incomplete rows"):
            self.__filtering_manager.get_for_target(feature_store, targets_table, feature_list, ["f1", "f2"], False)

    def test_skip_incomplete_rows(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2018, 1, 1), "c2f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", "EMPTY", "str")
                ),
                FeatureInstance(
                    self.__entity.name, "f2", "this is feature 2", "string", {}, FeatureTemplate("f2", "this is feature 2", "EMPTY", "str")
                ),
            ]
        )

        targets_table = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1)],
                ["2", dt.datetime(2018, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column],
        )

        features_data = self.__filtering_manager.get_for_target(feature_store, targets_table, feature_list, ["f1", "f2"], True)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column, self.__entity.time_column])


if __name__ == "__main__":
    unittest.main()
