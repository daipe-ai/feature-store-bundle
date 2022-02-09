from abc import ABC, abstractmethod
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList


class MetadataWriterInterface(ABC):
    @abstractmethod
    def write(self, entity: Entity, feature_list: FeatureList):
        pass
