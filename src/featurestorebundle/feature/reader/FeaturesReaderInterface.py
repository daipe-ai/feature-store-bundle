from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class FeaturesReaderInterface(ABC):
    @abstractmethod
    def read(self, entity_name: str) -> DataFrame:
        pass

    @abstractmethod
    def exists(self, entity_name: str) -> bool:
        pass
