import unittest
import datetime as dt
from pyspark.sql import types as t
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.delta.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.delta.feature.schema import get_feature_store_initial_schema
from pysparkbundle.test.PySparkTestCase import PySparkTestCase


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
        self.__features_preparer: FeaturesPreparer = self.__container.get(FeaturesPreparer)
        self.__feature_store_merge_columns = [self.__entity.id_column, self.__entity.time_column]

    def test_simple(self):
        features_storage = FeaturesStorage(self.__entity)

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="f1 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="f1 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f2",
                    description="f2 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f2",
                        description_template="f2 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage.add(df_1, feature_list_1)

        features_data = self.__features_preparer.prepare(features_storage)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)

    def test_two_feature_results(self):
        features_storage = FeaturesStorage(self.__entity)

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="f1 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="f1 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f2",
                    description="f2 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f2",
                        description_template="f2 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f3"],
                ["2", dt.datetime(2020, 1, 1), "c2f3"],
                ["3", dt.datetime(2020, 1, 1), "c3f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_2 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="f3 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="f3 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data = self.__features_preparer.prepare(features_storage)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2", "c1f3"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2", "c2f3"],
                ["3", dt.datetime(2020, 1, 1), "EMPTY", "EMPTY", "c3f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)

    def test_add_new_feature(self):
        features_storage = FeaturesStorage(self.__entity)

        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 2), "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="f3 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="f3 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage.add(df_1, feature_list_1)

        features_data = self.__features_preparer.prepare(features_storage)

        feature_store_after_merge = self.delta_merge(feature_store, features_data, self.__feature_store_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2", None],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2", None],
                ["1", dt.datetime(2020, 1, 2), None, None, "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(feature_store_after_merge, expected_feature_store, self.__feature_store_merge_columns)

    def test_backfill_feature(self):
        features_storage = FeaturesStorage(self.__entity)

        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2", None],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2", None],
                ["1", dt.datetime(2020, 1, 2), None, None, "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="f3 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="f3 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage.add(df_1, feature_list_1)

        features_data = self.__features_preparer.prepare(features_storage)

        feature_store_after_merge = self.delta_merge(feature_store, features_data, self.__feature_store_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2", "c1f3"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2", None],
                ["1", dt.datetime(2020, 1, 2), None, None, "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(feature_store_after_merge, expected_feature_store, self.__feature_store_merge_columns)

    def test_overwrite_feature(self):
        features_storage = FeaturesStorage(self.__entity)

        feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2", "c2f3"],
                ["1", dt.datetime(2020, 1, 2), "c1f1", "c1f2", "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "xxx"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="f3 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="f3 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 2), "xxx"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1"],
        )

        feature_list_2 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="f1 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="f1 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data = self.__features_preparer.prepare(features_storage)

        feature_store_after_merge = self.delta_merge(feature_store, features_data, self.__feature_store_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "EMPTY", "c1f2", "xxx"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2", "c2f3"],
                ["1", dt.datetime(2020, 1, 2), "xxx", "c1f2", "EMPTY"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(feature_store_after_merge, expected_feature_store, self.__feature_store_merge_columns)

    def test_dynamic_timestamp(self):
        features_storage = FeaturesStorage(self.__entity)

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="f1 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="f1 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f2",
                    description="f2 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f2",
                        description_template="f2 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 2), "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_2 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="f3 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="f3 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data = self.__features_preparer.prepare(features_storage)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2", "EMPTY"],
                ["2", dt.datetime(2020, 1, 1), "c2f1", "c2f2", "EMPTY"],
                ["1", dt.datetime(2020, 1, 2), "EMPTY", "EMPTY", "c1f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)

    def test_nulls_handled_correctly(self):
        feature_store = self.spark.createDataFrame([], schema=get_feature_store_initial_schema(self.__entity))

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 1), None, None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="f1 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="f1 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f2",
                    description="f2 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f2",
                        description_template="f2 description",
                        fillna_value=None,
                        fillna_value_type="NoneType",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage_1 = FeaturesStorage(self.__entity)
        features_storage_1.add(df_1, feature_list_1)
        features_data_1 = self.__features_preparer.prepare(features_storage_1)

        feature_store = self.delta_merge(feature_store, features_data_1, self.__feature_store_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", {0: "c1f2"}],
                ["2", dt.datetime(2020, 1, 1), "EMPTY", {0: None}],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.compare_dataframes(feature_store, expected_feature_store, self.__feature_store_merge_columns)

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 2), "c1f3"],
                ["2", dt.datetime(2020, 1, 2), None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_2 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="f3 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="f3 description",
                        fillna_value=None,
                        fillna_value_type="NoneType",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage_2 = FeaturesStorage(self.__entity)
        features_storage_2.add(df_2, feature_list_2)
        features_data_2 = self.__features_preparer.prepare(features_storage_2)

        feature_store = self.delta_merge(feature_store, features_data_2, self.__feature_store_merge_columns)

        expected_feature_store = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", {0: "c1f2"}, None],
                ["2", dt.datetime(2020, 1, 1), "EMPTY", {0: None}, None],
                ["1", dt.datetime(2020, 1, 2), None, None, {0: "c1f3"}],
                ["2", dt.datetime(2020, 1, 2), None, None, {0: None}],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(feature_store, expected_feature_store, self.__feature_store_merge_columns)

    def test_more_complicated(self):
        features_storage = FeaturesStorage(self.__entity)

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2"],
                ["2", dt.datetime(2020, 1, 2), "c2f1", "c2f2"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        feature_list_1 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="f1 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="f1 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f2",
                    description="f2 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f2",
                        description_template="f2 description",
                        fillna_value="EMPTY",
                        fillna_value_type="str",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 3), "c1f3"],
                ["2", dt.datetime(2020, 1, 2), "c2f3"],
                ["3", dt.datetime(2020, 1, 3), "c3f3"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        feature_list_2 = FeatureList(
            self.__entity,
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="f3 description",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="f3 description",
                        fillna_value=None,
                        fillna_value_type="NoneType",
                        location="datalake/path",
                        backend="delta_table",
                        notebook="test_notebook",
                        category="test_category",
                        owner="test_owner",
                        tags=["feature"],
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ],
        )

        features_storage.add(df_1, feature_list_1)
        features_storage.add(df_2, feature_list_2)

        features_data = self.__features_preparer.prepare(features_storage)

        expected_features_data = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "c1f1", "c1f2", {0: None}],
                ["2", dt.datetime(2020, 1, 2), "c2f1", "c2f2", {0: "c2f3"}],
                ["1", dt.datetime(2020, 1, 3), "EMPTY", "EMPTY", {0: "c1f3"}],
                ["3", dt.datetime(2020, 1, 3), "EMPTY", "EMPTY", {0: "c3f3"}],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(features_data, expected_features_data, self.__feature_store_merge_columns)


if __name__ == "__main__":
    unittest.main()
