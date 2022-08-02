from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from logging import Logger
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.PathExistenceChecker import PathExistenceChecker
from featurestorebundle.utils.errors import MissingTargetsEnumTableError, MissingTargetsTableError
from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface


class DeltaPathTargetsReader(TargetsReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        spark: SparkSession,
        path_existence_checker: PathExistenceChecker,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__path_existence_checker = path_existence_checker

    def read(self, entity_name: str) -> DataFrame:
        path = self.__table_names.get_targets_path(entity_name)
        if not self.enum_exists(entity_name):
            raise MissingTargetsTableError(
                f"Targets table at `{path}` does not exist. Targets table must be created before running this code"
            )

        self.__logger.info(f"Reading targets for entity {entity_name} from path {path}")

        return self.__spark.read.format("delta").load(path)

    def read_enum(self, entity_name: str) -> DataFrame:
        path = self.__table_names.get_targets_enum_path(entity_name)
        if not self.enum_exists(entity_name):
            raise MissingTargetsEnumTableError(
                f"Targets Enum table at `{path}` does not exist. Targets Enum table must be created before running this code"
            )

        return self.__spark.read.format("delta").load(path)

    def exists(self, entity_name: str) -> bool:
        path = self.__table_names.get_targets_path(entity_name)

        return self.__path_existence_checker.exists(path)

    def enum_exists(self, entity_name: str) -> bool:
        path = self.__table_names.get_targets_enum_path(entity_name)

        return self.__path_existence_checker.exists(path)
