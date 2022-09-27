from pyspark.sql import types as t
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.entity.EntityGetter import EntityGetter


class DummyEntityGetter(EntityGetter):
    def get(self) -> Entity:
        return Entity(
            name="client_test",
            id_column="client_id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )
