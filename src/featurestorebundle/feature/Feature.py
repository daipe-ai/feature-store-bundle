from typing import Dict, List, Union

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate
from featurestorebundle.metadata.DescriptionFiller import DescriptionFiller


class Feature:
    def __init__(self, name: str, description: str, dtype: str, extra: Dict, template: FeatureTemplate):
        self.__name = name
        self.__description = description
        self.__dtype = dtype
        self.__extra = extra
        self.__template = template

    @classmethod
    def from_template(cls, feature_template: FeatureTemplate, name: str, dtype: str, metadata: Dict[str, str]):
        filler = DescriptionFiller()
        description = feature_template.description_template.format(**{key: filler.format(key, val) for key, val in metadata.items()})
        return cls(name, description, dtype, metadata, feature_template)

    @property
    def name(self):
        return self.__name

    @property
    def description(self):
        return self.__description

    @property
    def dtype(self):
        return self.__dtype

    @property
    def extra(self):
        return self.__extra

    @property
    def template(self):
        return self.__template

    def get_metadata_dict(self) -> Dict[str, Union[str, Dict[str, str]]]:
        return {
            "name": self.__name,
            "description": self.__description,
            "extra": self.__extra,
            "template": self.__template.name_template,
            "category": self.__template.category,
            "dtype": self.__dtype,
        }

    def get_metadata_list(self) -> List[Union[Dict[str, str], str]]:
        return list(self.get_metadata_dict().values())

    def is_change_feature(self) -> bool:
        return isinstance(self.__template, FeatureWithChangeTemplate)
