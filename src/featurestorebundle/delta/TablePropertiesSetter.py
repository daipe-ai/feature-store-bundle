from typing import Dict

from featurestorebundle.delta.TablePropertySetter import TablePropertySetter


class TablePropertiesSetter:
    def __init__(self, table_property_setter: TablePropertySetter) -> None:
        self.__table_property_setter = table_property_setter

    def set_properties(self, table_identifier: str, properties: Dict[str, str]):
        for name, value in properties.items():
            self.__table_property_setter.set_property(table_identifier=table_identifier, property_name=name, property_value=value)
