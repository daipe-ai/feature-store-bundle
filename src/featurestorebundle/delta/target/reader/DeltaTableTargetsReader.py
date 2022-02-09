from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from logging import Logger
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface


class DeltaTableTargetsReader(TargetsReaderInterface):
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
        full_table_name = self.__table_names.get_entity_targets_full_table_name(entity)

        self.__logger.info(f"Reading targets for entity {entity} from table {full_table_name}")

        return self.__spark.read.table(full_table_name)
