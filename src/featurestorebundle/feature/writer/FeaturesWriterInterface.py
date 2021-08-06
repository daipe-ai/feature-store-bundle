from abc import ABC, abstractmethod
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage


class FeaturesWriterInterface(ABC):
    @abstractmethod
    def write_latest(self, features_storage: FeaturesStorage):
        pass

    @abstractmethod
    def write_historized(self, features_storage: FeaturesStorage):
        pass
