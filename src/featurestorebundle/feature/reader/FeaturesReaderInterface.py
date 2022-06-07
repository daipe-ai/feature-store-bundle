from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class FeaturesReaderInterface(ABC):
    @abstractmethod
    def read(self, location: str) -> DataFrame:
        pass

    @abstractmethod
    def get_backend(self) -> str:
        pass
