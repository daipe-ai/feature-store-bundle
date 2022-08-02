from typing import Optional
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class MetadataReaderInterface(ABC):
    @abstractmethod
    def read(self, entity_name: Optional[str]) -> DataFrame:
        pass
