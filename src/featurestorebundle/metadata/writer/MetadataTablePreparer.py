from pyspark.sql import SparkSession
from featurestorebundle.table.TableCreator import TableCreator
from featurestorebundle.table.TableSchemaMerger import TableSchemaMerger
from featurestorebundle.metadata.schema import get_metadata_schema, get_metadata_pk_columns


class MetadataTablePreparer:
    def __init__(
        self,
        table_creator: TableCreator,
        table_schema_merger: TableSchemaMerger,
        spark: SparkSession,
    ):
        self.__table_creator = table_creator
        self.__table_schema_merger = table_schema_merger
        self.__spark = spark

    def prepare(self, full_table_name: str, path: str):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_table_name.split('.')[0]}")
        self.__table_creator.create_if_not_exists(full_table_name, path, get_metadata_schema(), get_metadata_pk_columns()[0].name)
        self.__table_schema_merger.merge(full_table_name, get_metadata_schema())
