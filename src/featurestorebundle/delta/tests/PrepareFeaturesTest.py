from typing import List

import os
import pyspark.sql.types as t
import unittest
from featurestorebundle.feature.FeaturesPreparer import FeaturesPreparer
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.entity.Entity import Entity

os.environ["APP_ENV"] = "test"


class FakeResult:
    def __init__(self, columns: List[str], calls=None):
        self.schema = columns
        if calls is None:
            self.calls = {method: 0 for method in dir(FakeResult) if not method.startswith("__")}
        else:
            self.calls = calls

    def cache(self):
        self.calls["cache"] += 1
        return self

    def persist(self):
        self.calls["persist"] += 1
        return self

    def _jdf(self):
        self.calls["_jdf"] += 1
        return self

    def join(self, other, on, how):
        self.calls["join"] += 1
        new_cols = list(set(self.schema + other.schema))
        return FakeResult(new_cols, self.calls)

    def distinct(self):
        self.calls["distinct"] += 1
        return self

    def select(self, cols):
        self.calls["select"] += 1
        return FakeResult(cols, self.calls)

    def unionByName(self, other):  # noqa N802
        self.calls["unionByName"] += 1
        return self


class PrepareFeaturesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.__entity = Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="run_date",
            time_column_type=t.DateType(),
        )

        self.__container = bootstrapped_container.init("test")
        self.__features_preparer: FeaturesPreparer = self.__container.get(FeaturesPreparer)
        self.__dataframes = [
            FakeResult(["client_id", "run_date", "test1"]),
            FakeResult(["client_id", "run_date", "test2", "test3"]),
        ]

    def test_no_results(self):
        with self.assertRaises(Exception):
            self.__features_preparer.prepare(self.__entity, [])

    def test_two_dataframes(self):
        result: FakeResult = self.__features_preparer.prepare(self.__entity, self.__dataframes)
        self.assertEqual(1, result.calls["select"])
        self.assertEqual(1, result.calls["cache"])
        self.assertEqual({"test2", "client_id", "test1", "run_date", "test3"}, set(result.schema))


if __name__ == "__main__":
    unittest.main()
