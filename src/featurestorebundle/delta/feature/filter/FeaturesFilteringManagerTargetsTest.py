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


class FeaturesFilteringManagerTargetsTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )

        self.__feature_list = FeatureList(
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="",
                        fillna_value="",
                        fillna_value_type="str",
                        location="loc",
                        backend="bck",
                        notebook="ntb",
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f2",
                    description="",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f2",
                        description_template="",
                        fillna_value="",
                        location="loc",
                        backend="bck",
                        notebook="ntb",
                        fillna_value_type="str",
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ]
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

        targets_table = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1)],
                ["2", dt.datetime(2018, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column],
        )

        features_data = self.__filtering_manager.get_for_target(feature_store, targets_table, self.__feature_list, False)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2018, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__entity.get_primary_key())

    def test_incomplete_rows_raise_exception(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2018, 1, 1), "c2f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        targets_table = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1)],
                ["2", dt.datetime(2018, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column],
        )

        with self.assertRaisesRegex(Exception, "Features contain incomplete rows"):
            self.__filtering_manager.get_for_target(feature_store, targets_table, self.__feature_list, False)

    def test_skip_incomplete_rows(self):
        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2018, 1, 1), "c2f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        targets_table = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1)],
                ["2", dt.datetime(2018, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column],
        )

        features_data = self.__filtering_manager.get_for_target(feature_store, targets_table, self.__feature_list, True)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__entity.get_primary_key())


if __name__ == "__main__":
    unittest.main()
