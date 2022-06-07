from pyspark.sql import DataFrame
from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface


class TargetsReader:
    def __init__(self, targets_reader: TargetsReaderInterface):
        self.__targets_reader = targets_reader

    def read(self, entity_name: str) -> DataFrame:
        return self.__targets_reader.read(entity_name)

    def read_enum(self) -> DataFrame:
        return self.__targets_reader.read_enum()

    def exists(self, entity_name: str) -> bool:
        return self.__targets_reader.exists(entity_name)

    def enum_exists(self) -> bool:
        return self.__targets_reader.enum_exists()
