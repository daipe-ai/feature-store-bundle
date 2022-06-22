import os
from pyspark.sql import types as t
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from daipecore.decorator.notebook_function import notebook_function
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.notebook.decorator.feature import feature

os.environ["APP_ENV"] = "test"


class FakeSchema:
    json = {
        "fields": [
            {"name": "client_id", "type": "long", "metadata": {}},
            {"name": "timestamp", "type": "timestamp", "metadata": {}},
            {"name": "my_sample_feature", "type": "long", "metadata": {}},
        ],
    }

    # pylint: disable=invalid-name
    @staticmethod
    def jsonValue():
        return FakeSchema.json


class FakeResult:

    columns = ["client_id", "timestamp", "my_sample_feature"]

    def __init__(self, value):
        self.value = value
        self.schema = FakeSchema

    # pylint: disable=unused-argument
    def select(self, *args, **kwargs):
        return self

    # pylint: disable=unused-argument
    def fillna(self, *args, **kwargs):
        return self


entity = Entity(
    name="client_test",
    id_column="client_id",
    id_column_type=t.StringType(),
    time_column="timestamp",
    time_column_type=t.TimestampType(),
)


# pylint: disable=invalid-name
@DecoratedDecorator
class client_feature(feature):
    def __init__(self, *args, category=None):
        super().__init__(*args, entity=entity, category=category)


expected_value = FakeResult("not_a_real_dataframe")

try:

    @notebook_function()
    @client_feature(Feature("my_sample_feature", "my_sample_description", 0), category="test")
    def my_sample_feature():
        return expected_value

    assert expected_value == my_sample_feature.result

except ModuleNotFoundError as e:
    assert str(e) == "No module named 'IPython'"
