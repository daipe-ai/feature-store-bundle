from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.notebook.decorator.feature import feature


def create(entity: Entity):
    if f"{entity.name}_feature_decorator" in globals():
        return globals()[f"{entity.name}_feature_decorator"]

    @DecoratedDecorator
    class feature_decorator(feature):  # noqa # pylint: disable=invalid-name
        def __init__(self, *args, category=None, owner=None, tags=None, start_date=None, frequency=None):
            super().__init__(
                *args,
                entity=entity,
                category=category,
                owner=owner,
                tags=tags,
                start_date=start_date,
                frequency=frequency,
            )

    globals()[f"{entity.name}_feature_decorator"] = feature_decorator

    return feature_decorator
