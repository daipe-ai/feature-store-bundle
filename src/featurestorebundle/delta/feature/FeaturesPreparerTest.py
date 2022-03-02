import unittest
import hashlib
import datetime as dt
from pyspark.sql import types as t
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.delta.feature.FeaturesJoiner import FeaturesJoiner
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.delta.feature.schema import get_feature_store_initial_schema, get_rainbow_table_schema
from featurestorebundle.test.PySparkTestCase import PySparkTestCase


class FeaturesPreparerTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )

        self.__container = bootstrapped_container.init("test")
        self.__features_preparer: FeaturesJoiner = self.__container.get(FeaturesJoiner)
        self.__feature_store_merge_columns = [self.__entity.id_column, self.__entity.time_column]
        self.__rainbow_table_merge_columns = ["features_hash"]

    def test_simple(self):
        features_storage = FeaturesStorage(self.__entity)
        feature_store = self.spark.createDataFrame([], get_feature_store_initial_schema(self.__entity))
        rainbow_table = self.spark.createDataFrame([], get_rainbow_table_schema())

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.date(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", None)
                ),
                FeatureInstance(
                    self.__entity.name, "f2", "this is feature 2", "string", {}, FeatureTemplate("f2", "this is feature 2", None)
                ),
            ]
        )

        features_storage.add(df_1, feature_list_1)

        features_data, rainbow_data = self.__features_preparer.join(features_storage, feature_store, rainbow_table)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c1f1", "c1f2"],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2"],
        )

        expected_rainbow_data = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2".encode()).hexdigest(), ["f1", "f2"]],
            ],
            ["features_hash", "computed_columns"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)
        self.compare_dataframes(rainbow_data, expected_rainbow_data, self.__rainbow_table_merge_columns)

    def test_two_feature_results(self):
        features_storage = FeaturesStorage(self.__entity)
        feature_store = self.spark.createDataFrame([], get_feature_store_initial_schema(self.__entity))
        rainbow_table = self.spark.createDataFrame([], get_rainbow_table_schema())

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.date(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", None)
                ),
                FeatureInstance(
                    self.__entity.name, "f2", "this is feature 2", "string", {}, FeatureTemplate("f2", "this is feature 2", None)
                ),
            ]
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f3"],
                ["2", dt.date(2020, 1, 1), "c2f3"],
                ["3", dt.date(2020, 1, 1), "c3f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_2 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f3", "this is feature 3", "string", {}, FeatureTemplate("f3", "this is feature 3", None)
                ),
            ]
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data, rainbow_data = self.__features_preparer.join(features_storage, feature_store, rainbow_table)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c1f1", "c1f2", "c1f3"],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c2f1", "c2f2", "c2f3"],
                ["3", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), None, None, "c3f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        expected_rainbow_data = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2`f3".encode()).hexdigest(), ["f1", "f2", "f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)
        self.compare_dataframes(rainbow_data, expected_rainbow_data, self.__rainbow_table_merge_columns)

    def test_add_new_feature(self):
        features_storage = FeaturesStorage(self.__entity)

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

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 2), "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_1 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f3", "this is feature 3", "string", {}, FeatureTemplate("f3", "this is feature 3", None)
                ),
            ]
        )

        features_storage.add(df_1, feature_list_1)

        features_data, rainbow_data = self.__features_preparer.join(features_storage, feature_store, rainbow_table)

        feature_store_after_merge = self.delta_merge(feature_store, features_data, self.__feature_store_merge_columns)
        rainbow_table_after_merge = self.delta_merge(rainbow_table, rainbow_data, self.__rainbow_table_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c1f1", "c1f2", None],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c2f1", "c2f2", None],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f3".encode()).hexdigest(), None, None, "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        expected_rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2".encode()).hexdigest(), ["f1", "f2"]],
                [hashlib.md5("f3".encode()).hexdigest(), ["f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        self.compare_dataframes(feature_store_after_merge, expected_feature_store, self.__feature_store_merge_columns)
        self.compare_dataframes(rainbow_table_after_merge, expected_rainbow_table, self.__rainbow_table_merge_columns)

    def test_backfill_feature(self):
        features_storage = FeaturesStorage(self.__entity)

        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c1f1", "c1f2", None],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c2f1", "c2f2", None],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f3".encode()).hexdigest(), None, None, "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2".encode()).hexdigest(), ["f1", "f2"]],
                [hashlib.md5("f3".encode()).hexdigest(), ["f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_1 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f3", "this is feature 3", "string", {}, FeatureTemplate("f3", "this is feature 3", None)
                ),
            ]
        )

        features_storage.add(df_1, feature_list_1)

        features_data, rainbow_data = self.__features_preparer.join(features_storage, feature_store, rainbow_table)

        feature_store_after_merge = self.delta_merge(feature_store, features_data, self.__feature_store_merge_columns)
        rainbow_table_after_merge = self.delta_merge(rainbow_table, rainbow_data, self.__rainbow_table_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c1f1", "c1f2", "c1f3"],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2".encode()).hexdigest(), "c2f1", "c2f2", None],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f3".encode()).hexdigest(), None, None, "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        expected_rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2`f3".encode()).hexdigest(), ["f1", "f2", "f3"]],
                [hashlib.md5("f1`f2".encode()).hexdigest(), ["f1", "f2"]],
                [hashlib.md5("f3".encode()).hexdigest(), ["f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        self.compare_dataframes(feature_store_after_merge, expected_feature_store, self.__feature_store_merge_columns)
        self.compare_dataframes(rainbow_table_after_merge, expected_rainbow_table, self.__rainbow_table_merge_columns)

    def test_overwrite_feature(self):
        features_storage = FeaturesStorage(self.__entity)

        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c1f1", "c1f2", "c1f2"],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c2f1", "c2f2", "c2f3"],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c1f1", "c1f2", "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2`f3".encode()).hexdigest(), ["f1", "f2", "f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "xxx"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_1 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f3", "this is feature 3", "string", {}, FeatureTemplate("f3", "this is feature 3", None)
                ),
            ]
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 2), "xxx"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1"],
        )

        feature_list_2 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", None)
                ),
            ]
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data, rainbow_data = self.__features_preparer.join(features_storage, feature_store, rainbow_table)

        feature_store_after_merge = self.delta_merge(feature_store, features_data, self.__feature_store_merge_columns)
        rainbow_table_after_merge = self.delta_merge(rainbow_table, rainbow_data, self.__rainbow_table_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), None, "c1f2", "xxx"],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c2f1", "c2f2", "c2f3"],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "xxx", "c1f2", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        expected_rainbow_table = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2`f3".encode()).hexdigest(), ["f1", "f2", "f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        self.compare_dataframes(feature_store_after_merge, expected_feature_store, self.__feature_store_merge_columns)
        self.compare_dataframes(rainbow_table_after_merge, expected_rainbow_table, self.__rainbow_table_merge_columns)

    def test_dynamic_timestamp(self):
        features_storage = FeaturesStorage(self.__entity)
        feature_store = self.spark.createDataFrame([], get_feature_store_initial_schema(self.__entity))
        rainbow_table = self.spark.createDataFrame([], get_rainbow_table_schema())

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.date(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", None)
                ),
                FeatureInstance(
                    self.__entity.name, "f2", "this is feature 2", "string", {}, FeatureTemplate("f2", "this is feature 2", None)
                ),
            ]
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 2), "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_2 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f3", "this is feature 3", "string", {}, FeatureTemplate("f3", "this is feature 3", None)
                ),
            ]
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data, rainbow_data = self.__features_preparer.join(features_storage, feature_store, rainbow_table)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c1f1", "c1f2", None],
                ["2", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c2f1", "c2f2", None],
                ["1", dt.date(2020, 1, 2), hashlib.md5("f1`f2`f3".encode()).hexdigest(), None, None, "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        expected_rainbow_data = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2`f3".encode()).hexdigest(), ["f1", "f2", "f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)
        self.compare_dataframes(rainbow_data, expected_rainbow_data, self.__rainbow_table_merge_columns)

    def test_more_complicated(self):
        features_storage = FeaturesStorage(self.__entity)
        feature_store = self.spark.createDataFrame([], get_feature_store_initial_schema(self.__entity))
        rainbow_table = self.spark.createDataFrame([], get_rainbow_table_schema())

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.date(2020, 1, 2), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f1", "this is feature 1", "string", {}, FeatureTemplate("f1", "this is feature 1", None)
                ),
                FeatureInstance(
                    self.__entity.name, "f2", "this is feature 2", "string", {}, FeatureTemplate("f2", "this is feature 2", None)
                ),
            ]
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 3), "c1f3"],
                ["2", dt.date(2020, 1, 2), "c2f3"],
                ["3", dt.date(2020, 1, 3), "c3f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_2 = FeatureList(
            [
                FeatureInstance(
                    self.__entity.name, "f3", "this is feature 3", "string", {}, FeatureTemplate("f3", "this is feature 3", None)
                ),
            ]
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data, rainbow_data = self.__features_preparer.join(features_storage, feature_store, rainbow_table)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c1f1", "c1f2", None],
                ["2", dt.date(2020, 1, 2), hashlib.md5("f1`f2`f3".encode()).hexdigest(), "c2f1", "c2f2", "c2f3"],
                ["1", dt.date(2020, 1, 3), hashlib.md5("f1`f2`f3".encode()).hexdigest(), None, None, "c1f3"],
                ["3", dt.date(2020, 1, 3), hashlib.md5("f1`f2`f3".encode()).hexdigest(), None, None, "c3f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "features_hash", "f1", "f2", "f3"],
        )

        expected_rainbow_data = self.spark.createDataFrame(
            [
                [hashlib.md5("f1`f2`f3".encode()).hexdigest(), ["f1", "f2", "f3"]],
            ],
            ["features_hash", "computed_columns"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)
        self.compare_dataframes(rainbow_data, expected_rainbow_data, self.__rainbow_table_merge_columns)


if __name__ == "__main__":
    unittest.main()
