import re
from featurestorebundle.feature.FeatureList import FeatureList


class FeatureNamesValidator:
    def validate(self, feature_list: FeatureList):
        for feature_name in feature_list.get_names():
            if not re.match(r"^[a-zA-Z0-9_]+$", feature_name):
                raise Exception(f"Invalid feature name '{feature_name}', only alphanumeric characters and underscores are allowed")
