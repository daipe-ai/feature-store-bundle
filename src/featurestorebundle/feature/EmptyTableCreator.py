from pyspark.sql import SparkSession
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.Entity import Entity


class EmptyTableCreator:
    def __init__(self, spark: SparkSession, table_names: TableNames):
        self.__spark = spark
        self.__table_names = table_names

    def create(self, entity: Entity):
        def build_create_entity_table_string(entity: Entity):
            return (
                f"CREATE TABLE IF NOT EXISTS {self.__table_names.get_full_tablename(entity.name)}\n"
                f'({entity.id_column} {entity.id_column_type.typeName()} COMMENT "Entity id column",\n'
                f'{entity.time_column} {entity.time_column_type.typeName()} COMMENT "Compute time column")\n'
                f"USING DELTA\n"
                f"LOCATION '{self.__table_names.get_path(entity.name)}'\n"
                f"PARTITIONED BY ({entity.time_column})\n"
                f'COMMENT "The table contains entity {entity.name} features"\n'
            )

        self.__spark.sql(build_create_entity_table_string(entity))
