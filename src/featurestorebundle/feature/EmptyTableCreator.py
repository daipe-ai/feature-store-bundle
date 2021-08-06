from pyspark.sql import SparkSession
from featurestorebundle.entity.Entity import Entity


class EmptyTableCreator:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def create(self, table_name: str, path: str, entity: Entity):
        def build_create_entity_table_string(entity: Entity):
            return (
                f"CREATE TABLE IF NOT EXISTS {table_name}\n"
                f'({entity.id_column} {entity.id_column_type.typeName()} NOT NULL COMMENT "Entity id column",\n'
                f'{entity.time_column} {entity.time_column_type.typeName()} NOT NULL COMMENT "Compute time column")\n'
                f"USING DELTA\n"
                f"LOCATION '{path}'\n"
                f'COMMENT "The table contains entity {entity.name} features"\n'
            )

        self.__spark.sql(build_create_entity_table_string(entity))
