import datetime as dt
import os
import unittest

from pyspark.sql import types as t
from daipecore.decorator.notebook_function import notebook_function

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.notebook.decorator import feature_decorator_factory
from featurestorebundle.test.PySparkTestCase import PySparkTestCase

os.environ["APP_ENV"] = "test"


class NullHandlerTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )
        self.__feature_decorator = feature_decorator_factory.create(self.__entity)

    def test_nulls_after_decorator(self):
        @notebook_function()
        @self.__feature_decorator(
            Feature("f1", "f1 description", "default"),
            Feature("f2", "f2 description", None),
        )
        def test():
            return self.spark.createDataFrame(
                [
                    ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                    ["3", dt.date(2020, 1, 1), None, None],
                    ["4", dt.date(2020, 1, 1), "c4f1", None],
                ],
                [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
            )

        expected_df = self.spark.createDataFrame(
            [
                ["1", dt.date(2020, 1, 1), "c1f1", "c1f2"],
                ["3", dt.date(2020, 1, 1), "default", None],
                ["4", dt.date(2020, 1, 1), "c4f1", None],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        self.assertEqual(expected_df.collect(), test.result.collect())


if __name__ == "__main__":
    unittest.main()
