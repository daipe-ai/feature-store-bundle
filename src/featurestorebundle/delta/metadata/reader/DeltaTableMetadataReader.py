from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from logging import Logger
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.metadata.reader.MetadataReaderInterface import MetadataReaderInterface


class DeltaTableMetadataReader(MetadataReaderInterface):
    def __init__(
        self,
        logger: Logger,
        table_names: TableNames,
        spark: SparkSession,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names

    def read(self, entity_name: Optional[str] = None) -> DataFrame:
        entity_name = entity_name or ""
        full_table_name = self.__table_names.get_metadata_full_table_name(entity_name)

        self.__logger.info(f"Reading metadata from table {full_table_name}")

        df = self.__spark.read.table(full_table_name)

        if entity_name:
            df = df.filter(f.col("entity") == entity_name)

        return df
