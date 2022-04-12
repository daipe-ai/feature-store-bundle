from typing import Optional
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.notebook.decorator.feature import feature


def create(entity: Entity, features_storage: Optional[FeaturesStorage] = None):
    if f"{entity.name}_feature_decorator" in globals():
        return globals()[f"{entity.name}_feature_decorator"]

    # pylint: disable=invalid-name
    @DecoratedDecorator
    class feature_decorator(feature):  # noqa
        def __init__(self, *args, category=None, owner=None, start_date=None, frequency=None):
            super().__init__(
                *args,
                entity=entity,
                category=category,
                owner=owner,
                start_date=start_date,
                frequency=frequency,
                features_storage=features_storage,
            )

    globals()[f"{entity.name}_feature_decorator"] = feature_decorator

    return feature_decorator
