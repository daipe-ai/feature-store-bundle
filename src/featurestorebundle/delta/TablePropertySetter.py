from pyspark.sql import SparkSession


class TablePropertySetter:
    def __init__(self, spark: SparkSession) -> None:
        self.__spark = spark

    def set_property(self, table_identifier: str, property_name: str, property_value: str):
        self.__spark.sql(f"ALTER TABLE {table_identifier} SET TBLPROPERTIES ('{property_name}' = '{property_value}');")
