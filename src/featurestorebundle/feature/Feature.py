from pyspark.sql.types import DataType


class Feature:
    def __init__(self, name: str, description: str, dtype: DataType, category: str = None):

        self.__name = name
        self.__description = description
        self.__dtype = dtype
        self.__category = category

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
    def category(self):
        return self.__category
