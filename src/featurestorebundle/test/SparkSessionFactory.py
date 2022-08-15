from pyspark.sql import SparkSession


class SparkSessionFactory:
    def create(self):
        return SparkSession.builder.master("local[1]").getOrCreate()
