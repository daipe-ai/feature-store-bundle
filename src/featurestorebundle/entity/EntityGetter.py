from box import Box
from pyspark.sql import types as t
from daipecore.widgets.Widgets import Widgets
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.utils.types import names_to_dtypes
from featurestorebundle.widgets.WidgetNames import WidgetNames


class EntityGetter:

    _allowed_id_column_types = ["string", "integer", "long", "short"]

    def __init__(self, entities: Box, widgets: Widgets, widget_names: WidgetNames):
        self.__entities = entities
        self.__widgets = widgets
        self.__widget_names = widget_names

    def get(self) -> Entity:
        if len(self.__entities) > 1:
            entity_name = self.__widgets.get_value(self.__widget_names.entity_name)
        else:
            entity_name = list(self.__entities)[0]

        entity = self.__find_entity_by_name(entity_name)

        return self.__create_entity(entity_name, entity.id_column, entity.id_column_type)

    def get_by_name(self, entity_name: str) -> Entity:
        entity = self.__find_entity_by_name(entity_name)

        return self.__create_entity(entity_name, entity.id_column, entity.id_column_type)

    def __create_entity(self, entity_name: str, id_column: str, id_column_type: str) -> Entity:
        if id_column_type not in self._allowed_id_column_types:
            raise Exception(f"Invalid id column type, allowed types are {self._allowed_id_column_types}")

        return Entity(
            entity_name,
            id_column,
            names_to_dtypes[id_column_type],
            "timestamp",
            t.TimestampType(),
        )

    def __find_entity_by_name(self, entity_name: str) -> Box:
        for entity in self.__entities:
            if entity_name == entity:
                return self.__entities[entity]

        raise Exception(f"Cannot find entity {entity_name} in config")
