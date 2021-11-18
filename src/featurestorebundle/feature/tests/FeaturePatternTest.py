import unittest
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeaturePattern import FeaturePattern


class FeaturePatternTest(unittest.TestCase):
    def test_simple(self):
        template = FeatureTemplate("test_feature", "test description")
        pattern = FeaturePattern(template)
        match = pattern.get_match("test_feature")

        self.assertIsNotNone(match)

    def test_placeholders(self):
        template = FeatureTemplate("test_{arg1}_{arg2}_something", "test description")
        pattern = FeaturePattern(template)
        match = pattern.get_match("test_hello_world_something")

        self.assertIsNotNone(match)
        self.assertDictEqual({"arg1": "hello", "arg2": "world"}, pattern.get_groups_as_dict(match))

    def test_appedix(self):
        template = FeatureTemplate("test_something", "test description")
        pattern = FeaturePattern(template)
        match = pattern.get_match("test_something_else")

        self.assertIsNone(match)

        template = FeatureTemplate("test_{arg1}_{arg2}_something", "test description")
        pattern = FeaturePattern(template)
        match = pattern.get_match("test_hello_world_something_else")

        self.assertIsNone(match)


if __name__ == "__main__":
    unittest.main()
