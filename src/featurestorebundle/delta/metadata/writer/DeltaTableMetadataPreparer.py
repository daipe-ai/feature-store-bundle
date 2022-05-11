from pyspark.sql import SparkSession
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.delta.TableCreator import TableCreator
from featurestorebundle.delta.TableSchemaMerger import TableSchemaMerger
from featurestorebundle.delta.metadata.schema import get_metadata_schema, get_metadata_pk_columns


class DeltaTableMetadataPreparer:
    def __init__(
        self,
        table_names: TableNames,
        table_creator: TableCreator,
        table_schema_merger: TableSchemaMerger,
        spark: SparkSession,
    ):
        self.__table_names = table_names
        self.__table_creator = table_creator
        self.__table_schema_merger = table_schema_merger
        self.__spark = spark

    def prepare(self, full_table_name: str, path: str):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.__table_names.get_db_name()}")
        self.__table_creator.create_if_not_exists(full_table_name, path, get_metadata_schema(), get_metadata_pk_columns()[0].name)
        self.__table_schema_merger.merge(full_table_name, get_metadata_schema())
