from typing import List, Optional
from injecta.container.ContainerInterface import ContainerInterface
from daipecore.function.input_decorator_function import input_decorator_function
from featurestorebundle.feature.FeatureStore import FeatureStore


@input_decorator_function
def read_features(entity_name: str, feature_names: Optional[List[str]] = None, historized: bool = False):
    def wrapper(container: ContainerInterface):
        feature_store: FeatureStore = container.get(FeatureStore)

        if historized:
            return feature_store.get_historized(entity_name, feature_names)
        return feature_store.get_latest(entity_name, feature_names)

    return wrapper
