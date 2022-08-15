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
from pysparkbundle.test.PySparkTestCase import PySparkTestCase

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
        feature_with_change_template = FeatureWithChangeTemplate(
            name_template="feature_{time_window}",
            description_template="feature in {time_window}",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        template = FeatureTemplate(
            name_template="a_{time_window}",
            description_template="a in {time_window}",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        features_with_change = [
            FeatureInstance(
                "entity", "feature_14d", "feature in 14 days", "int", "numerical", {"time_window": "14d"}, feature_with_change_template
            ),
            FeatureInstance(
                "entity", "feature_30d", "feature in 30 days", "int", "numerical", {"time_window": "30d"}, feature_with_change_template
            ),
        ]

        feature_list = FeatureList(
            self.__entity,
            [
                *features_with_change,
                FeatureInstance("entity", "a_14d", "a in 14 days", "int", "numerical", {"time_window": "14d"}, template),
            ],
        )

        change_features = feature_list.get_change_features()

        self.assertListEqual([MasterFeature("feature_{time_window}", features_with_change, ["14d", "30d"])], change_features)

    def test_time_window_in_the_middle(self):
        feature_with_change_template = FeatureWithChangeTemplate(
            name_template="feature_{time_window}_suffix",
            description_template="feature suffix in {time_window}",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        template = FeatureTemplate(
            name_template="a_{time_window}",
            description_template="a in {time_window}",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        features_with_change = [
            FeatureInstance(
                "entity",
                "feature_14d_suffix",
                "feature suffix in 14 days",
                "int",
                "numerical",
                {"time_window": "14d"},
                feature_with_change_template,
            ),
            FeatureInstance(
                "entity",
                "feature_30d_suffix",
                "feature suffix in 30 days",
                "int",
                "numerical",
                {"time_window": "30d"},
                feature_with_change_template,
            ),
        ]

        feature_list = FeatureList(
            self.__entity,
            [
                *features_with_change,
                FeatureInstance("entity", "a_14d", "a in 14 days", "int", "numerical", {"time_window": "14d"}, template),
            ],
        )

        change_features = feature_list.get_change_features()

        self.assertListEqual([MasterFeature("feature_{time_window}_suffix", features_with_change, ["14d", "30d"])], change_features)

    def test_wrong_name(self):
        feature_with_change_template = FeatureWithChangeTemplate(
            name_template="{time_window}_suffix",
            description_template="feature suffix in {time_window}",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        template = FeatureTemplate(
            name_template="a_{time_window}",
            description_template="a in {time_window}",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        features_with_change = [
            FeatureInstance(
                "entity",
                "14d_suffix",
                "feature suffix in 14 days",
                "int",
                "numerical",
                {"time_window": "14d"},
                feature_with_change_template,
            ),
            FeatureInstance(
                "entity",
                "30d_suffix",
                "feature suffix in 30 days",
                "int",
                "numerical",
                {"time_window": "30d"},
                feature_with_change_template,
            ),
        ]

        feature_list = FeatureList(
            self.__entity,
            [
                *features_with_change,
                FeatureInstance("entity", "a_14d", "a in 14 days", "int", "numerical", {"time_window": "14d"}, template),
            ],
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
