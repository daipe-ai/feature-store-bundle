import os
import datetime as dt
from pyspark.sql import SparkSession
from daipecore.decorator.notebook_function import notebook_function
from pysparkbundle.test.PySparkTestCase import PySparkTestCase
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.notebook.decorator.feature import feature

os.environ["APP_ENV"] = "test"

spark = SparkSession.builder.master("local[1]").getOrCreate()

expected_dataframe = spark.createDataFrame([["1", dt.datetime(2020, 1, 1), 123]], ["client_id", "timestamp", "my_sample_feature"])


@notebook_function()
@feature(Feature("my_sample_feature", "my_sample_description", 0))
def my_sample_feature():
    return expected_dataframe


PySparkTestCase().compare_dataframes(my_sample_feature.result, expected_dataframe, sort_keys=["client_id"])
