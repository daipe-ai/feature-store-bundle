from typing import Dict, List, Union

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate
from featurestorebundle.metadata.DescriptionFiller import DescriptionFiller
from featurestorebundle.utils.TypeChecker import TypeChecker


class FeatureInstance:
    def __init__(self, entity: str, name: str, description: str, dtype: str, variable_type: str, extra: Dict, template: FeatureTemplate):
        self.__entity = entity
        self.__name = name
        self.__description = description
        self.__dtype = dtype
        self.__variable_type = variable_type
        self.__extra = extra
        self.__template = template

    @classmethod
    def from_template(
        cls, feature_template: FeatureTemplate, entity: str, name: str, dtype: str, variable_type: str, extra: Dict[str, str]
    ):
        type_checker = TypeChecker()
        type_checker.check(feature_template, dtype, variable_type)

        filler = DescriptionFiller()
        description = feature_template.description_template.format(**{key: filler.format(key, val) for key, val in extra.items()})
        return cls(entity, name, description, dtype, variable_type, extra, feature_template)

    @property
    def entity(self) -> str:
        return self.__entity

    @property
    def name(self) -> str:
        return self.__name

    @property
    def description(self) -> str:
        return self.__description

    @property
    def dtype(self) -> str:
        return self.__dtype

    @property
    def variable_type(self) -> str:
        return self.__variable_type

    @property
    def storage_dtype(self) -> str:
        return f"map<integer,{self.__dtype}>" if self.__template.fillna_value is None else self.__dtype

    @property
    def extra(self) -> Dict[str, str]:
        return self.__extra

    @property
    def template(self) -> FeatureTemplate:
        return self.__template

    def get_metadata_dict(self) -> Dict[str, Union[str, Dict[str, str]]]:
        return {
            "entity": self.__entity,
            "name": self.__name,
            "description": self.__description,
            "extra": self.__extra,
            "feature_template": self.__template.name_template,
            "description_template": self.__template.description_template,
            "category": self.__template.category,
            "owner": self.__template.owner,
            "tags": self.__template.tags,
            "start_date": self.__template.start_date,
            "frequency": self.__template.frequency,
            "last_compute_date": self.__template.last_compute_date,
            "dtype": self.__dtype,
            "variable_type": self.__variable_type,
            "fillna_value": str(self.__template.fillna_value),
            "fillna_value_type": self.__template.fillna_value_type,
            "location": self.__template.location,
            "backend": self.__template.backend,
            "notebook_name": self.__template.notebook_name,
            "notebook_absolute_path": self.__template.notebook_absolute_path,
            "notebook_relative_path": self.__template.notebook_relative_path,
        }

    def get_metadata_list(self) -> List[Union[Dict[str, str], str]]:
        return list(self.get_metadata_dict().values())

    def is_change_feature(self) -> bool:
        return isinstance(self.__template, FeatureWithChangeTemplate)
