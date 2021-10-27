from typing import Optional


class FeatureTemplate:
    def __init__(self, name_template: str, description_template: str, category: Optional[str] = None):

        self.__name_template = name_template
        self.__description_template = description_template
        self.__category = category

    @property
    def name_template(self):
        return self.__name_template

    @property
    def description_template(self):
        return self.__description_template

    @property
    def category(self):
        return self.__category
