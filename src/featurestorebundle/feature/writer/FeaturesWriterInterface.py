from abc import ABC, abstractmethod
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage


class FeaturesWriterInterface(ABC):
    @abstractmethod
    def write(self, features_storage: FeaturesStorage):
        pass
