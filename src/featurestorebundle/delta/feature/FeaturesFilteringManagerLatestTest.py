import unittest
import datetime as dt
from pyspark.sql import types as t
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.delta.feature.FeaturesFilteringManager import FeaturesFilteringManager
from featurestorebundle.test.PySparkTestCase import PySparkTestCase


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

        features_data = self.__filtering_manager.get_latest(feature_store, ["f1", "f2"], False)

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

        with self.assertRaisesRegex(Exception, "Features f3 not registered"):
            self.__filtering_manager.get_latest(feature_store, ["f3"], False)

    def test_null_returned_correctly(self):
        feature_store = self.spark.createDataFrame(
            [
                ["2", dt.datetime(2020, 1, 1), {0: "c2f1"}, None, None],
                ["1", dt.datetime(2020, 1, 1), {0: "c1f1"}, None, None],
                ["1", dt.datetime(2020, 1, 2), {0: None}, "c1f2", "c1f3"],
            ],
            f"{self.__entity.id_column} string, {self.__entity.time_column} timestamp, f1 map<byte,string>, f2 string, f3 string",
        )

        features_data = self.__filtering_manager.get_latest(feature_store, ["f1", "f2", "f3"], True)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", {0: None}, "c1f2", "c1f3"],
            ],
            f"{self.__entity.id_column} string, f1 map<byte,string>, f2 string, f3 string",
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])

    def test_types_are_preserved(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), {0: 111}, None, None],
                ["2", dt.datetime(2020, 1, 1), {0: 222}, None, None],
                ["1", dt.datetime(2020, 1, 2), {0: None}, 333, 1.12345678910111213],
            ],
            f"{self.__entity.id_column} string, {self.__entity.time_column} timestamp, f1 map<byte,int>, f2 int, f3 double",
        )

        features_data = self.__filtering_manager.get_latest(feature_store, ["f1", "f2", "f3"], True)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", {0: None}, 333, 1.12345678910111213],
            ],
            f"{self.__entity.id_column} string, f1 map<byte,int>, f2 int, f3 double",
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])


if __name__ == "__main__":
    unittest.main()
