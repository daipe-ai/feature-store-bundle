from typing import Dict, List

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


class Feature:
    def __init__(self, name: str, description: str, dtype: str, extra: Dict, template: FeatureTemplate):
        self.__name = name
        self.__description = description
        self.__dtype = dtype
        self.__extra = extra
        self.__template = template

    @property
    def name(self):
        return self.__name

    @property
    def description(self):
        return self.__description

    @property
    def dtype(self):
        return self.__dtype

    def get_metadata_dict(self) -> Dict[str, str]:
        return {
            "name": self.__name,
            "description": self.__description,
            "extra": self.__extra,
            "template": self.__template.name_template,
            "category": self.__template.category,
            "dtype": self.__dtype,
        }

    def get_metadata_list(self) -> List[str]:
        return list(self.get_metadata_dict().values())
