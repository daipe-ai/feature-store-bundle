from abc import ABC, abstractmethod
from featurestorebundle.feature.FeatureList import FeatureList


class MetadataWriterInterface(ABC):
    @abstractmethod
    def write(self, feature_list: FeatureList):
        pass
