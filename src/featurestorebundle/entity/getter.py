from daipecore.decorator.ContainerManager import ContainerManager
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.entity.Entity import Entity


def get_entity() -> Entity:
    container = ContainerManager.get_container()
    entity_getter: EntityGetter = container.get(EntityGetter)

    return entity_getter.get()
