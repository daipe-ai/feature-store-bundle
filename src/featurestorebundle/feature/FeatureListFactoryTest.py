import unittest
import datetime as dt
from pyspark.sql import types as t
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory
from featurestorebundle.delta.metadata.schema import get_metadata_schema
from pysparkbundle.test.PySparkTestCase import PySparkTestCase


class FeaturesListFactoryTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )

        self.__container = bootstrapped_container.init("test")
        self.__feature_list_factory: FeatureListFactory = self.__container.get(FeatureListFactory)

    def test_simple(self):
        metadata = self.spark.createDataFrame(
            [
                [
                    self.__entity.name,
                    "f1",
                    "desc1",
                    {},
                    "f1",
                    "desc1",
                    "cat1",
                    "ow1",
                    dt.datetime(2020, 1, 1),
                    "daily",
                    dt.datetime(2020, 1, 1),
                    "string",
                    "categorical",
                    "",
                    "str",
                    False,
                    "loc",
                    "bck",
                    "ntb",
                ],
                [
                    self.__entity.name,
                    "f2",
                    "desc2",
                    {},
                    "f2",
                    "desc2",
                    "cat2",
                    "ow2",
                    dt.datetime(2020, 1, 1),
                    "daily",
                    dt.datetime(2020, 1, 1),
                    "integer",
                    "numerical",
                    0,
                    "int",
                    False,
                    "loc",
                    "bck",
                    "ntb",
                ],
                [
                    self.__entity.name,
                    "f3",
                    "desc3",
                    {},
                    "f3",
                    "desc3",
                    "cat3",
                    "ow3",
                    dt.datetime(2020, 1, 1),
                    "daily",
                    dt.datetime(2020, 1, 1),
                    "string",
                    "categorical",
                    "None",
                    "NoneType",
                    False,
                    "loc",
                    "bck",
                    "ntb",
                ],
            ],
            get_metadata_schema(),
        )

        feature_list = self.__feature_list_factory.create(metadata)

        expected_feature_list = FeatureList(
            [
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f1",
                    description="desc1",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f1",
                        description_template="desc1",
                        fillna_value="",
                        fillna_value_type="str",
                        location="loc",
                        backend="bck",
                        notebook="ntb",
                        category="cat1",
                        owner="ow1",
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f2",
                    description="desc2",
                    dtype="integer",
                    variable_type="numerical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f2",
                        description_template="desc2",
                        fillna_value=0,
                        fillna_value_type="int",
                        location="loc",
                        backend="bck",
                        notebook="ntb",
                        category="cat2",
                        owner="ow2",
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
                FeatureInstance(
                    entity=self.__entity.name,
                    name="f3",
                    description="desc3",
                    dtype="string",
                    variable_type="categorical",
                    extra={},
                    template=FeatureTemplate(
                        name_template="f3",
                        description_template="desc3",
                        fillna_value=None,
                        fillna_value_type="NoneType",
                        location="loc",
                        backend="bck",
                        notebook="ntb",
                        category="cat3",
                        owner="ow3",
                        start_date=dt.datetime(2020, 1, 1),
                        frequency="daily",
                        last_compute_date=dt.datetime(2020, 1, 1),
                    ),
                ),
            ]
        )

        for feature1, feature2 in zip(feature_list.get_all(), expected_feature_list.get_all()):
            self.assertEqual(feature1.entity, feature2.entity)
            self.assertEqual(feature1.description, feature2.description)
            self.assertEqual(feature1.dtype, feature2.dtype)
            self.assertEqual(feature1.variable_type, feature2.variable_type)
            self.assertEqual(feature1.extra, feature2.extra)
            self.assertEqual(feature1.template.name_template, feature2.template.name_template)
            self.assertEqual(feature1.template.description_template, feature2.template.description_template)
            self.assertEqual(feature1.template.fillna_value, feature2.template.fillna_value)
            self.assertEqual(feature1.template.fillna_value_type, feature2.template.fillna_value_type)
            self.assertEqual(feature1.template.location, feature2.template.location)
            self.assertEqual(feature1.template.backend, feature2.template.backend)
            self.assertEqual(feature1.template.notebook, feature2.template.notebook)
            self.assertEqual(feature1.template.category, feature2.template.category)
            self.assertEqual(feature1.template.owner, feature2.template.owner)
            self.assertEqual(feature1.template.start_date, feature2.template.start_date)
            self.assertEqual(feature1.template.frequency, feature2.template.frequency)
            self.assertEqual(feature1.template.last_compute_date, feature2.template.last_compute_date)


if __name__ == "__main__":
    unittest.main()
