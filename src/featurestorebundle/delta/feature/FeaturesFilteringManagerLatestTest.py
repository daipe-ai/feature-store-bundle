import unittest
import hashlib
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
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c1f1", "c1f2"],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2"],
        )

        rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2".encode()).hexdigest(), ["f1", "f2"]],
            ],
            ["features_hash", "computed_columns"],
        )

        features_data = self.__filtering_manager.get_latest(feature_store, rainbow_table, ["f1", "f2"])

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
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c1f1", "c1f2"],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2"],
        )

        rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2".encode()).hexdigest(), ["f1", "f2"]],
            ],
            ["features_hash", "computed_columns"],
        )

        with self.assertRaisesRegex(Exception, "Features f3 not registered"):
            self.__filtering_manager.get_latest(feature_store, rainbow_table, ["f3"])

    def test_uncomputed_feature_wont_be_returned(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1".encode()).hexdigest(), "c1f1", None, None],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1".encode()).hexdigest(), "c2f1", None, None],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f3".encode()).hexdigest(), None, None, "c1f3"],
            ],
            f"{self.__entity.id_column} string, {self.__entity.time_column} date, features_hash string, f1 string, f2 string, f3 string",
        )

        rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1".encode()).hexdigest(), ["f1"]],
                [hashlib.md5("f3".encode()).hexdigest(), ["f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        features_data = self.__filtering_manager.get_latest(feature_store, rainbow_table, ["f1", "f2", "f3"])

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", "c1f1", "c1f3"],
            ],
            f"{self.__entity.id_column} string, f1 string, f3 string",
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])

    def test_null_returned_correctly(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1".encode()).hexdigest(), "c1f1", None, None],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1".encode()).hexdigest(), "c2f1", None, None],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f1`f3".encode()).hexdigest(), None, None, "c1f3"],
            ],
            f"{self.__entity.id_column} string, {self.__entity.time_column} date, features_hash string, f1 string, f2 string, f3 string",
        )

        rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1".encode()).hexdigest(), ["f1"]],
                [hashlib.md5("f3".encode()).hexdigest(), ["f3"]],
                [hashlib.md5("f1`f3".encode()).hexdigest(), ["f1", "f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        features_data = self.__filtering_manager.get_latest(feature_store, rainbow_table, ["f1", "f2", "f3"])

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", None, "c1f3"],
            ],
            f"{self.__entity.id_column} string, f1 string, f3 string",
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])

    def test_types_are_preserved(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1".encode()).hexdigest(), 111, None, None],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1".encode()).hexdigest(), 222, None, None],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f1`f3".encode()).hexdigest(), None, None, 1.12345678910111213],
            ],
            f"{self.__entity.id_column} string, {self.__entity.time_column} date, features_hash string, f1 int, f2 int, f3 double",
        )

        rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1".encode()).hexdigest(), ["f1"]],
                [hashlib.md5("f1`f3".encode()).hexdigest(), ["f1", "f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        features_data = self.__filtering_manager.get_latest(feature_store, rainbow_table, ["f1", "f2", "f3"])

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", None, 1.12345678910111213],
            ],
            f"{self.__entity.id_column} string, f1 int, f3 double",
        )

        self.compare_dataframes(features_data, expected_features_data, [self.__entity.id_column])


if __name__ == "__main__":
    unittest.main()
