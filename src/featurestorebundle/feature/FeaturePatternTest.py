import unittest
import datetime as dt
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeaturePattern import FeaturePattern


class FeaturePatternTest(unittest.TestCase):
    def test_simple(self):
        template = FeatureTemplate(
            name_template="test_feature",
            description_template="test description",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        pattern = FeaturePattern(template)
        match = pattern.get_match("test_feature")

        self.assertIsNotNone(match)

    def test_placeholders(self):
        template = FeatureTemplate(
            name_template="test_{arg1}_{arg2}_something",
            description_template="test description",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        pattern = FeaturePattern(template)
        match = pattern.get_match("test_hello_world_something")

        self.assertIsNotNone(match)
        self.assertDictEqual({"arg1": "hello", "arg2": "world"}, pattern.get_groups_as_dict(match))

    def test_appedix(self):
        template = FeatureTemplate(
            name_template="test_something",
            description_template="test description",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        pattern = FeaturePattern(template)
        match = pattern.get_match("test_something_else")

        self.assertIsNone(match)

        template = FeatureTemplate(
            name_template="test_{arg1}_{arg2}_something",
            description_template="test description",
            fillna_value=0,
            fillna_value_type="int",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )

        pattern = FeaturePattern(template)
        match = pattern.get_match("test_hello_world_something_else")

        self.assertIsNone(match)


if __name__ == "__main__":
    unittest.main()
