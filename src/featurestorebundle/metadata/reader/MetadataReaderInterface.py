from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class MetadataReaderInterface(ABC):
    @abstractmethod
    def read(self) -> DataFrame:
        pass
