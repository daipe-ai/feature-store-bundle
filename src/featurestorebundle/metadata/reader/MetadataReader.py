from typing import Optional
from pyspark.sql import DataFrame
from featurestorebundle.metadata.reader.MetadataReaderInterface import MetadataReaderInterface


class MetadataReader:
    def __init__(self, metadata_reader: MetadataReaderInterface):
        self.__metadata_reader = metadata_reader

    def read(self, entity_name: Optional[str]) -> DataFrame:
        return self.__metadata_reader.read(entity_name)
