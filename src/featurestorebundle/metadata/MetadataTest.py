import unittest
import datetime as dt

from pyspark.sql import types as t

from featurestorebundle.delta.metadata.schema import get_metadata_schema
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.metadata.MetadataValidator import MetadataValidator
from pysparkbundle.test.PySparkTestCase import PySparkTestCase


class MetadataTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )

    def test_metadata(self):
        feature_list = FeatureList(
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

        df = self.spark.createDataFrame(feature_list.get_metadata(), get_metadata_schema())

        expected_df = self.spark.createDataFrame(
            feature_list.get_metadata(),
            t.StructType(
                [
                    t.StructField("entity", t.StringType(), False),
                    t.StructField("feature", t.StringType(), False),
                    t.StructField("description", t.StringType(), True),
                    t.StructField("extra", t.MapType(t.StringType(), t.StringType()), True),
                    t.StructField("feature_template", t.StringType(), True),
                    t.StructField("description_template", t.StringType(), True),
                    t.StructField("category", t.StringType(), True),
                    t.StructField("owner", t.StringType(), True),
                    t.StructField("tags", t.ArrayType(t.StringType()), True),
                    t.StructField("start_date", t.TimestampType(), True),
                    t.StructField("frequency", t.StringType(), True),
                    t.StructField("last_compute_date", t.TimestampType(), True),
                    t.StructField("dtype", t.StringType(), True),
                    t.StructField("variable_type", t.StringType(), True),
                    t.StructField("fillna_value", t.StringType(), True),
                    t.StructField("fillna_value_type", t.StringType(), True),
                    t.StructField("location", t.StringType(), True),
                    t.StructField("backend", t.StringType(), True),
                    t.StructField("notebook", t.StringType(), True),
                ]
            ),
        )

        self.compare_dataframes(expected_df, df, ["entity"])

    def test_metadata_validator_fields(self):
        feature_template = FeatureTemplate("a", "b", "c", "d", "e", "f", "g", "h", "i", [], dt.datetime.min, "k", dt.datetime.min)
        self.assertTrue(
            # pylint: disable=protected-access
            all((hasattr(feature_template, field) for field in MetadataValidator._immutable_metadata_template_fields))
        )

        feature_instance = FeatureInstance("a", "b", "c", "d", "e", {}, feature_template)
        self.assertTrue(
            # pylint: disable=protected-access
            all((hasattr(feature_instance, field) for field in MetadataValidator._immutable_metadata_instance_fields))
        )


if __name__ == "__main__":
    unittest.main()
