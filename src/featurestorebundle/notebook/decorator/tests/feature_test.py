import os
import pyspark.sql.types as t
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from daipecore.decorator.notebook_function import notebook_function
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.notebook.decorator.feature import feature

os.environ["APP_ENV"] = "test"


class FakeResult:

    columns = ["client_id", "run_date"]

    def __init__(self, value):
        self.value = value


entity = Entity(
    name="client_test",
    id_column="client_id",
    id_column_type=t.StringType(),
    time_column="run_date",
    time_column_type=t.DateType(),
)

features_storage = FeaturesStorage(entity)


@DecoratedDecorator
class client_feature(feature):  # noqa: N801
    def __init__(self, *args, category=None):
        super().__init__(*args, entity=entity, category=category, features_storage=features_storage)


expected_value = FakeResult("not_a_real_dataframe")


@notebook_function()
@client_feature("my_sample_table", t.IntegerType())
def my_sample_feature():
    return expected_value


assert expected_value == features_storage.results[0]