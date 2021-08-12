import re

from featurestorebundle.feature.Feature import Feature


class FeaturePattern:
    def __init__(self, feature: Feature):

        self.__feature = feature
        self.__placeholders = re.findall(r"{(\w+)}", feature.name)
        placeholder_translations = {placeholder: f"(?P<{placeholder}>.+)" for placeholder in self.__placeholders}
        self.__pattern = re.compile(feature.name.format(**placeholder_translations))

    @property
    def feature(self):
        return self.__feature

    @property
    def pattern(self):
        return self.__pattern

    @property
    def placeholders(self):
        return self.__placeholders
