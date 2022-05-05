import unittest
import datetime as dt
from pyspark.sql import types as t
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.delta.join.LeftWithCheckpointingJoiner import LeftWithCheckpointingJoiner
from featurestorebundle.delta.join.UnionWithWindowJoiner import UnionWithWindowJoiner
from featurestorebundle.checkpoint.CheckpointDirHandler import CheckpointDirHandler
from pysparkbundle.test.PySparkTestCase import PySparkTestCase


class DataFrameJoinerTest(PySparkTestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )

        self.__container = bootstrapped_container.init("test")
        self.__checkpoint_dir_handler: CheckpointDirHandler = self.__container.get(CheckpointDirHandler)
        self.__left_with_checkpointing_joiner = LeftWithCheckpointingJoiner(9999, self.__checkpoint_dir_handler)
        self.__union_with_window_joiner = UnionWithWindowJoiner()

    def test_simple(self):
        df1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "A", "A"],
                ["2", dt.datetime(2020, 1, 1), "B", "B"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        df2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "X"],
                ["2", dt.datetime(2020, 1, 1), "Y"],
                ["3", dt.datetime(2020, 1, 1), "Z"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        expected_dataframe = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "A", "A", "X"],
                ["2", dt.datetime(2020, 1, 1), "B", "B", "Y"],
                ["3", dt.datetime(2020, 1, 1), None, None, "Z"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(
            self.__left_with_checkpointing_joiner.join([df1, df2], self.__entity.get_primary_key()),
            expected_dataframe,
            self.__entity.get_primary_key(),
        )

        self.compare_dataframes(
            self.__union_with_window_joiner.join([df1, df2], self.__entity.get_primary_key()),
            expected_dataframe,
            self.__entity.get_primary_key(),
        )

    def test_null_id(self):
        df1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "A", "A"],
                ["2", dt.datetime(2020, 1, 1), "B", "B"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2"],
        )

        df2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "X"],
                [None, dt.datetime(2020, 1, 1), "Y"],
                ["3", dt.datetime(2020, 1, 1), "Z"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f3"],
        )

        expected_dataframe = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 1, 1), "A", "A", "X"],
                ["2", dt.datetime(2020, 1, 1), "B", "B", None],
                ["3", dt.datetime(2020, 1, 1), None, None, "Z"],
            ],
            [self.__entity.id_column, self.__entity.time_column, "f1", "f2", "f3"],
        )

        self.compare_dataframes(
            self.__left_with_checkpointing_joiner.join([df1, df2], self.__entity.get_primary_key()),
            expected_dataframe,
            self.__entity.get_primary_key(),
        )

        self.compare_dataframes(
            self.__union_with_window_joiner.join([df1, df2], self.__entity.get_primary_key()),
            expected_dataframe,
            self.__entity.get_primary_key(),
        )


if __name__ == "__main__":
    unittest.main()
