from daipecore.decorator.ContainerManager import ContainerManager
from daipecore.widgets.Widgets import Widgets

from featurestorebundle.entity.Entity import Entity


def get_entity() -> Entity:
    container = ContainerManager.get_container()
    widgets: Widgets = container.get(Widgets)
    entities = container.get_parameters().featurestorebundle.entities

    if len(entities) > 1:
        selected_entity = widgets.get_value("entity")
    else:
        selected_entity = list(entities)[0]

    return Entity(selected_entity, entities[selected_entity].id_column, entities[selected_entity].id_column_type, "run_date", "date")
