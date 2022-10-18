import os
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from daipecore.decorator.notebook_function import notebook_function
from pysparkbundle.test.PySparkTestCase import PySparkTestCase
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.notebook.decorator.feature import feature

os.environ["APP_ENV"] = "test"


spark = SparkSession.builder.master("local[1]").getOrCreate()


entity = Entity(
    name="client_test",
    id_column="client_id",
    id_column_type=t.StringType(),
    time_column="timestamp",
    time_column_type=t.TimestampType(),
)


@DecoratedDecorator
class client_feature(feature):  # noqa # pylint: disable=invalid-name
    def __init__(self, *args):
        super().__init__(*args, entity=entity)


expected_dataframe = spark.createDataFrame([["1", dt.datetime(2020, 1, 1), 123]], ["client_id", "timestamp", "my_sample_feature"])


@notebook_function()
@client_feature(Feature("my_sample_feature", "my_sample_description", 0))
def my_sample_feature():
    return expected_dataframe


PySparkTestCase().compare_dataframes(my_sample_feature.result, expected_dataframe, sort_keys=["client_id"])
