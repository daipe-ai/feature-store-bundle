import unittest

from pyspark.sql import types as t

from featurestorebundle.delta.metadata.schema import get_metadata_schema
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
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
            [
                FeatureInstance(
                    self.__entity.name,
                    "f1",
                    "this is feature 1",
                    "string",
                    "categorical",
                    {},
                    FeatureTemplate("f1", "this is feature 1", None, "str", "loc", "bck", "ntb"),
                ),
                FeatureInstance(
                    self.__entity.name,
                    "f2",
                    "this is feature 2",
                    "string",
                    "categorical",
                    {},
                    FeatureTemplate("f2", "this is feature 2", None, "str", "loc", "bck", "ntb"),
                ),
            ]
        )

        df = self.spark.createDataFrame(feature_list.get_metadata(), get_metadata_schema())

        expected_df = self.spark.createDataFrame(
            feature_list.get_metadata(),
            t.StructType(
                [
                    t.StructField("entity", t.StringType(), False),
                    t.StructField("feature", t.StringType(), False),
                    t.StructField("description", t.StringType(), True),
                    t.StructField("extra", t.MapType(t.StringType(), t.StringType(), True)),
                    t.StructField("feature_template", t.StringType(), True),
                    t.StructField("description_template", t.StringType(), True),
                    t.StructField("category", t.StringType(), True),
                    t.StructField("owner", t.StringType(), True),
                    t.StructField("start_date", t.TimestampType(), True),
                    t.StructField("frequency", t.StringType(), True),
                    t.StructField("last_compute_date", t.TimestampType(), True),
                    t.StructField("dtype", t.StringType(), True),
                    t.StructField("variable_type", t.StringType(), True),
                    t.StructField("fillna_value", t.StringType(), True),
                    t.StructField("fillna_value_type", t.StringType(), True),
                    t.StructField("is_feature", t.BooleanType(), True),
                    t.StructField("location", t.StringType(), True),
                    t.StructField("backend", t.StringType(), True),
                    t.StructField("notebook", t.StringType(), True),
                ]
            ),
        )

        self.compare_dataframes(expected_df, df, ["entity"])


if __name__ == "__main__":
    unittest.main()
