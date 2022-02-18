from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class TargetsReaderInterface(ABC):
    @abstractmethod
    def read(self, entity_name: str) -> DataFrame:
        pass

    @abstractmethod
    def read_enum(self) -> DataFrame:
        pass
