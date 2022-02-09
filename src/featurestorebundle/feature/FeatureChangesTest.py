import unittest

from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureList import FeatureList, UnsupportedChangeFeatureNameException
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate
from featurestorebundle.feature.MasterFeature import MasterFeature


class FeatureChangesTest(unittest.TestCase):
    def test_simple(self):
        feature_with_change_template = FeatureWithChangeTemplate("feature_{time_window}", "feature in {time_window}")
        template = FeatureTemplate("a_{time_window}", "a in {time_window}")

        features_with_change = [
            Feature("entity", "feature_14d", "feature in 14 days", "int", {"time_window": "14d"}, feature_with_change_template),
            Feature("entity", "feature_30d", "feature in 30 days", "int", {"time_window": "30d"}, feature_with_change_template),
        ]

        feature_list = FeatureList(
            [
                *features_with_change,
                Feature("entity", "a_14d", "a in 14 days", "int", {"time_window": "14d"}, template),
            ]
        )

        change_features = feature_list.get_change_features()

        self.assertListEqual([MasterFeature("feature_{time_window}", features_with_change, ["14d", "30d"])], change_features)

    def test_time_window_in_the_middle(self):
        feature_with_change_template = FeatureWithChangeTemplate("feature_{time_window}_suffix", "feature suffix in {time_window}")
        template = FeatureTemplate("a_{time_window}", "a in {time_window}")

        features_with_change = [
            Feature(
                "entity", "feature_14d_suffix", "feature suffix in 14 days", "int", {"time_window": "14d"}, feature_with_change_template
            ),
            Feature(
                "entity", "feature_30d_suffix", "feature suffix in 30 days", "int", {"time_window": "30d"}, feature_with_change_template
            ),
        ]

        feature_list = FeatureList(
            [
                *features_with_change,
                Feature("entity", "a_14d", "a in 14 days", "int", {"time_window": "14d"}, template),
            ]
        )

        change_features = feature_list.get_change_features()

        self.assertListEqual([MasterFeature("feature_{time_window}_suffix", features_with_change, ["14d", "30d"])], change_features)

    def test_wrong_name(self):
        feature_with_change_template = FeatureWithChangeTemplate("{time_window}_suffix", "feature suffix in {time_window}")
        template = FeatureTemplate("a_{time_window}", "a in {time_window}")

        features_with_change = [
            Feature("entity", "14d_suffix", "feature suffix in 14 days", "int", {"time_window": "14d"}, feature_with_change_template),
            Feature("entity", "30d_suffix", "feature suffix in 30 days", "int", {"time_window": "30d"}, feature_with_change_template),
        ]

        feature_list = FeatureList(
            [
                *features_with_change,
                Feature("entity", "a_14d", "a in 14 days", "int", {"time_window": "14d"}, template),
            ]
        )

        self.assertRaises(UnsupportedChangeFeatureNameException, feature_list.get_change_features)


if __name__ == "__main__":
    unittest.main()
