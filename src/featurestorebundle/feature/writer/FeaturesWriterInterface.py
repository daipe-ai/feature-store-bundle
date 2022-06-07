from abc import ABC, abstractmethod
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage


class FeaturesWriterInterface(ABC):
    @abstractmethod
    def write(self, features_storage: FeaturesStorage):
        pass

    @abstractmethod
    def get_location(self, entity_name: str) -> str:
        pass

    @abstractmethod
    def get_backend(self) -> str:
        pass
