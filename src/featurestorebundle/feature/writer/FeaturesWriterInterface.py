from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage


class FeaturesWriterInterface(ABC):
    @abstractmethod
    def write(self, result: DataFrame, entity: Entity, feature_list: FeatureList):
        pass

    @abstractmethod
    def write_all(self, features_storage: FeaturesStorage):
        pass
