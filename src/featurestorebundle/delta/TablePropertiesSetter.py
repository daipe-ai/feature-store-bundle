from typing import List

from featurestorebundle.delta.TablePropertySetter import TablePropertySetter


class TablePropertiesSetter:
    def __init__(self, table_property_setter: TablePropertySetter) -> None:
        self.__table_property_setter = table_property_setter

    def set_properties(self, table_identifier: str, property_names: List[str], property_values: List[str]):
        if not self.__validate_arguments(property_names, property_values):
            raise Exception("Number of elements in property_names has to match with number of elements in property_values.")

        for name, value in zip(property_names, property_values):
            self.__table_property_setter.set_property(table_identifier=table_identifier, property_name=name, property_value=value)

    def __validate_arguments(self, arg1: List[str], arg2: List[str]) -> bool:
        return len(arg1) == len(arg2)
