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

    def read(self, entity: str) -> DataFrame:
        path = self.__table_names.get_entity_targets_path(entity)

        self.__logger.info(f"Reading targets for entity {entity} from path {path}")

        return self.__spark.read.format("delta").load(path)
