from pyspark.sql import SparkSession


class TableExistenceChecker:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def exists(self, full_table_name: str) -> bool:
        db_name = full_table_name.split(".")[0]
        table_name = full_table_name.split(".")[1]
        databases = [db.databaseName for db in self.__spark.sql("SHOW DATABASES").collect()]

        if db_name not in databases:
            return False

        return self.__spark.sql(f'SHOW TABLES IN {db_name} LIKE "{table_name}"').collect() != []
