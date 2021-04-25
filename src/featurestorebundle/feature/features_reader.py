from typing import List
from injecta.container.ContainerInterface import ContainerInterface
from daipecore.function.input_decorator_function import input_decorator_function
from featurestorebundle.feature.FeatureStore import FeatureStore


@input_decorator_function
def read_features(entity_name: str, feature_names: List[str] = None):
    def wrapper(container: ContainerInterface):
        feature_store: FeatureStore = container.get(FeatureStore)

        return feature_store.get(entity_name, feature_names)

    return wrapper
