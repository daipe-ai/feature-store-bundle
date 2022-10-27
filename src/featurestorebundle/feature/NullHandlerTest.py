import datetime as dt
import os
import unittest

from pyfonycore.bootstrap import bootstrapped_container
from daipecore.decorator.notebook_function import notebook_function

from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChange import FeatureWithChange
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.writer.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.notebook.decorator.feature import feature
from pysparkbundle.test.PySparkTestCase import PySparkTestCase
from featurestorebundle.utils.TypeChecker import TypeChecker
from featurestorebundle.utils.errors import WrongFillnaValueTypeError

os.environ["APP_ENV"] = "test"


class NullHandlerTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__container = bootstrapped_container.init("test")
        self.__entity_getter: EntityGetter = self.__container.get(EntityGetter)
        self.__features_preparer: FeaturesPreparer = self.__container.get(FeaturesPreparer)
        self.__type_checker: TypeChecker = TypeChecker()
        self.__entity = self.__entity_getter.get()

    def test_nulls_after_decorator(self):
        @notebook_function()
        @feature(
            Feature("f1", "f1 description", "default"),
            Feature("f2", "f2 description", None),
        )
        def test():
            return self.spark.createDataFrame(
                [
                    ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                    ["3", dt.datetime(2020, 1, 1), None, None],
                    ["4", dt.datetime(2020, 1, 1), "c4f1", None],
                ],
                [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
            )

        expected_df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["3", dt.datetime(2020, 1, 1), "default", None],
                ["4", dt.datetime(2020, 1, 1), "c4f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.compare_dataframes(expected_df, test.result, self.__entity.get_primary_key())

    def test_changes_null_values(self):
        string_default = "test"
        change_feature_default = 3
        change_default = 0.0
        count_default = 5

        @notebook_function()
        @feature(
            Feature("f1_cat_{time_window}", "f1 cat description {time_window}", string_default),
            FeatureWithChange("f1_sum_{time_window}", "f1 sum description {time_window}", change_feature_default),
        )
        def test1():
            return self.spark.createDataFrame(
                [
                    ["1", dt.datetime(2020, 3, 2), "hello", 100, "world", 400],
                    ["3", dt.datetime(2020, 3, 2), "hello", None, "world", 500],
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
        @feature(
            Feature("f2_count_{time_window}", "f2 description {time_window}", count_default),
        )
        def test2():
            return self.spark.createDataFrame(
                [
                    ["2", dt.datetime(2020, 3, 2), 1, None],
                ],
                schema=f"{self.__entity.id_column} string, {self.__entity.time_column} timestamp, f2_count_20d long, f2_count_40d long",
            )

        features_storage = FeaturesStorage(self.__entity)
        features_storage.add(test1.result, test1.previous_decorator_instance._feature__feature_list)  # pylint: disable=protected-access
        features_storage.add(test2.result, test2.previous_decorator_instance._feature__feature_list)  # pylint: disable=protected-access

        features_data = self.__features_preparer.prepare(features_storage)

        expected_df = self.spark.createDataFrame(
            [
                ["3", dt.datetime(2020, 3, 2), "hello", change_feature_default, "world", 500, change_default, count_default, count_default],
                [
                    "2",
                    dt.datetime(2020, 3, 2),
                    string_default,
                    change_feature_default,
                    string_default,
                    change_feature_default,
                    change_default,
                    1,
                    count_default,
                ],
                ["1", dt.datetime(2020, 3, 2), "hello", 100, "world", 400, 0.5, count_default, count_default],
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

    def test_type_checker_good(self):
        template = FeatureTemplate(
            name_template="test_bool",
            description_template="test_bool",
            fillna_value=False,
            fillna_value_type="bool",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "boolean", "binary")

        template = FeatureTemplate(
            name_template="test_int",
            description_template="test_int",
            fillna_value=0,
            fillna_value_type="int",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "integer", "numerical")

        template = FeatureTemplate(
            name_template="test_str",
            description_template="test_str",
            fillna_value="",
            fillna_value_type="str",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "string", "categorical")

        template = FeatureTemplate(
            name_template="test_date",
            description_template="test_date",
            fillna_value=dt.datetime.now(),
            fillna_value_type="datetime",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "date", None)

    def test_type_checker_bad(self):
        with self.assertRaises(WrongFillnaValueTypeError):
            template = FeatureTemplate(
                name_template="test_bool",
                description_template="test_bool",
                fillna_value=0,
                fillna_value_type="int",
                base_db="test_db",
                repository="https://test_repository.git",
                notebook_name="test_notebook",
                notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
                notebook_relative_path="test_folder/test_notebook",
                category="test_category",
                owner="test_owner",
                tags=["feature"],
                start_date=dt.datetime(2020, 1, 1),
                frequency="daily",
                last_compute_date=dt.datetime(2020, 1, 1),
            )

            self.__type_checker.check(template, "boolean", "binary")

        with self.assertRaises(WrongFillnaValueTypeError):
            template = FeatureTemplate(
                name_template="test_int",
                description_template="test_int",
                fillna_value="0",
                fillna_value_type="str",
                base_db="test_db",
                repository="https://test_repository.git",
                notebook_name="test_notebook",
                notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
                notebook_relative_path="test_folder/test_notebook",
                category="test_category",
                owner="test_owner",
                tags=["feature"],
                start_date=dt.datetime(2020, 1, 1),
                frequency="daily",
                last_compute_date=dt.datetime(2020, 1, 1),
            )

            self.__type_checker.check(template, "integer", "numerical")

        with self.assertRaises(WrongFillnaValueTypeError):
            template = FeatureTemplate(
                name_template="test_str",
                description_template="test_str",
                fillna_value=123,
                fillna_value_type="int",
                base_db="test_db",
                repository="https://test_repository.git",
                notebook_name="test_notebook",
                notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
                notebook_relative_path="test_folder/test_notebook",
                category="test_category",
                owner="test_owner",
                tags=["feature"],
                start_date=dt.datetime(2020, 1, 1),
                frequency="daily",
                last_compute_date=dt.datetime(2020, 1, 1),
            )

            self.__type_checker.check(template, "string", "categorical")

        with self.assertRaises(WrongFillnaValueTypeError):
            template = FeatureTemplate(
                name_template="test_date",
                description_template="test_date",
                fillna_value="2020-01-01",
                fillna_value_type="str",
                base_db="test_db",
                repository="https://test_repository.git",
                notebook_name="test_notebook",
                notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
                notebook_relative_path="test_folder/test_notebook",
                category="test_category",
                owner="test_owner",
                tags=["feature"],
                start_date=dt.datetime(2020, 1, 1),
                frequency="daily",
                last_compute_date=dt.datetime(2020, 1, 1),
            )

            self.__type_checker.check(template, "date", None)

    def test_type_checker_none(self):
        template = FeatureTemplate(
            name_template="test_bool",
            description_template="test_bool",
            fillna_value=None,
            fillna_value_type="NoneType",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "boolean", "binary")

        template = FeatureTemplate(
            name_template="test_int",
            description_template="test_int",
            fillna_value=None,
            fillna_value_type="NoneType",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "integer", "numerical")

        template = FeatureTemplate(
            name_template="test_str",
            description_template="test_str",
            fillna_value=None,
            fillna_value_type="NoneType",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "string", "categorical")

        template = FeatureTemplate(
            name_template="test_date",
            description_template="test_date",
            fillna_value=None,
            fillna_value_type="NoneType",
            base_db="test_db",
            repository="https://test_repository.git",
            notebook_name="test_notebook",
            notebook_absolute_path="/Repos/repository/test_folder/test_notebook",
            notebook_relative_path="test_folder/test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        self.__type_checker.check(template, "date", None)


if __name__ == "__main__":
    unittest.main()
