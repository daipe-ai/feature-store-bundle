import datetime as dt
import os
import sys
import unittest

from daipecore.decorator.notebook_function import notebook_function
from pyspark.sql import DataFrame, functions as f, types as t

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.notebook.functions.input_functions import with_timestamps
from pysparkbundle.test.PySparkTestCase import PySparkTestCase
from featurestorebundle.utils.errors import WrongColumnTypeError

os.environ["APP_ENV"] = "test"


class WithTimestampsTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )
        sys.argv.append("target_name=<no target>")
        sys.argv.append("timestamp=20201212")

    def test_with_timestamps_exception(self):
        df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "a"],
                ["3", dt.datetime(2020, 1, 1), "b"],
                ["1", dt.datetime(2020, 1, 2), "c"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "not_a_date_column"],
        )

        with self.assertRaises(WrongColumnTypeError):

            @notebook_function(
                with_timestamps(
                    df,
                    self.__entity,
                    "not_a_date_column",
                    ["90d"],
                )
            )
            def test(df: DataFrame):
                return df

    def test_with_timestamps_date(self):
        df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1)],
                ["3", dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1)],
                ["1", dt.datetime(2020, 1, 2), dt.datetime(2020, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column, "date"],
        )

        with self.assertRaises(Exception) as context:

            @notebook_function(
                with_timestamps(
                    df,
                    self.__entity,
                    "date",
                    ["90d"],
                )
            )
            def test(df: DataFrame):
                return df

        self.assertEqual('No widget defined for name "target_name"', str(context.exception))

    def test_with_timestamps_timestamp(self):
        df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1)],
                ["3", dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1)],
                ["1", dt.datetime(2020, 1, 2), dt.datetime(2020, 1, 1)],
            ],
            [self.__entity.id_column, self.__entity.time_column, "date"],
        ).withColumn("timestamp", f.col("date").cast("timestamp"))

        with self.assertRaises(Exception) as context:

            @notebook_function(
                with_timestamps(
                    df,
                    self.__entity,
                    "timestamp",
                    ["90d"],
                )
            )
            def test(df: DataFrame):
                return df

        self.assertEqual('No widget defined for name "target_name"', str(context.exception))


if __name__ == "__main__":
    unittest.main()
