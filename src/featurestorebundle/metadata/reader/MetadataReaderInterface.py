from abc import ABC, abstractmethod
from typing import Optional
from pyspark.sql import DataFrame


class MetadataReaderInterface(ABC):
    @abstractmethod
    def read(self, entity_name: Optional[str] = None) -> DataFrame:
        pass
