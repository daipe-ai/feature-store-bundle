import os
import pyspark.sql.types as t
import unittest
from collections import namedtuple
from featurestorebundle.delta.DeltaMergeConfigGenerator import DeltaMergeConfigGenerator
from featurestorebundle.entity.Entity import Entity

os.environ["APP_ENV"] = "test"


class FakeSchema:
    client_id = {"name": "client_id", "type": "long"}
    run_date = {"name": "run_date", "type": "long"}
    my_sample_feature = {"name": "my_sample_feature", "type": "long"}

    schema = {
        "fields": [
            namedtuple("ObjectName", client_id.keys())(*client_id.values()),
            namedtuple("ObjectName", run_date.keys())(*run_date.values()),
            namedtuple("ObjectName", my_sample_feature.keys())(*my_sample_feature.values()),
        ],
    }

    fields = schema["fields"]

    @staticmethod
    def jsonValue():  # noqa N802
        return FakeSchema.schema


class FakeResult:

    columns = ["client_id", "run_date", "my_sample_feature"]

    def __init__(self, value):
        self.value = value
        self.schema = FakeSchema


class DeltaMergeConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="run_date",
            time_column_type=t.DateType(),
        )
        fake_df = FakeResult("not_a_real_dataframe")

        self.__latest_config = DeltaMergeConfigGenerator().generate(self.entity, fake_df, [self.entity.id_column])
        self.__historized_config = DeltaMergeConfigGenerator().generate(
            self.entity, fake_df, [self.entity.id_column, self.entity.time_column]
        )

    def test_run_date_update_set(self):
        self.assertIn("run_date", self.__latest_config.update_set)
        self.assertNotIn("run_date", self.__historized_config.update_set)

    def test_run_date_insert_set(self):
        self.assertIn("run_date", self.__latest_config.insert_set)
        self.assertIn("run_date", self.__historized_config.insert_set)

    def test_client_id_update_set(self):
        self.assertNotIn("client_id", self.__latest_config.update_set)
        self.assertNotIn("client_id", self.__historized_config.update_set)

    def test_client_id_insert_set(self):
        self.assertIn("client_id", self.__latest_config.insert_set)
        self.assertIn("client_id", self.__historized_config.insert_set)

    def test_feature_in_both_sets(self):
        self.assertIn("my_sample_feature", self.__latest_config.insert_set)
        self.assertIn("my_sample_feature", self.__historized_config.insert_set)

        self.assertIn("my_sample_feature", self.__latest_config.update_set)
        self.assertIn("my_sample_feature", self.__historized_config.update_set)

    def test_latest_merge_condition(self):
        self.assertEqual("target.client_id = source.client_id", self.__latest_config.merge_condition)

    def test_historized_merge_condition(self):
        self.assertEqual(
            " AND ".join(f"target.{pk} = source.{pk}" for pk in [self.entity.id_column, self.entity.time_column]),
            self.__historized_config.merge_condition,
        )


if __name__ == "__main__":
    unittest.main()
