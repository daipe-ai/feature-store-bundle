import re
from typing import Dict

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


class FeaturePattern:
    def __init__(self, feature_template: FeatureTemplate):

        self.__feature_template = feature_template
        self.__placeholders = re.findall(r"{(\w+)}", feature_template.name_template)
        placeholder_translations = {placeholder: f"(?P<{placeholder}>.+)" for placeholder in self.__placeholders}
        self.__pattern = re.compile(feature_template.name_template.format(**placeholder_translations))

    @property
    def feature_template(self):
        return self.__feature_template

    def get_match(self, feature_name: str) -> re.Match:
        return self.__pattern.match(feature_name)

    def get_groups_as_dict(self, match: re.Match) -> Dict[str, str]:
        return {placeholder: match.group(placeholder) for placeholder in self.__placeholders}
