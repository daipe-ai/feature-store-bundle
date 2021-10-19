from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.metadata.schema import METADATA_SCHEMA


class MetadataTablePreparer:
    def __init__(
        self,
        table_names: TableNames,
        spark: SparkSession,
    ):
        self.__table_names = table_names
        self.__spark = spark

    def prepare(self, full_table_name: str, path: str, entity: Entity):
        self.__spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.__table_names.get_db_name(entity.name)}")

        if not self.__table_exists(full_table_name):
            metadata_df = self.__spark.createDataFrame(self.__spark.sparkContext.emptyRDD(), schema=METADATA_SCHEMA)
            metadata_df.write.format("delta").option("path", path).saveAsTable(full_table_name)

    def __table_exists(self, full_table_name: str) -> bool:
        sql_context = SQLContext(self.__spark.sparkContext)
        db_name = full_table_name.split(".")[0]
        table_name = full_table_name.split(".")[1]

        return table_name in sql_context.tableNames(db_name)
