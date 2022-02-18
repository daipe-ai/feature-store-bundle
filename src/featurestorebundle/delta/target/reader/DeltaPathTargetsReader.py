from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from logging import Logger
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface


class DeltaPathTargetsReader(TargetsReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        spark: SparkSession,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names

    def read(self, entity_name: str) -> DataFrame:
        path = self.__table_names.get_targets_path(entity_name)

        self.__logger.info(f"Reading targets for entity {entity_name} from path {path}")

        return self.__spark.read.format("delta").load(path)

    def read_enum(self) -> DataFrame:
        path = self.__table_names.get_targets_enum_path()

        return self.__spark.read.format("delta").load(path)
