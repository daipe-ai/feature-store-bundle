import pyspark.sql.types as t
from featurestorebundle.entity.Entity import Entity


def get_id_column_name(entity: Entity):
    return entity.id_column


def get_time_column_name(entity: Entity):
    return entity.time_column


def get_target_id_column_name():
    return "target_id"


def get_entity_targets_schema(entity: Entity):
    return [
        t.StructField(entity.id_column, entity.id_column_type, False),
        t.StructField(entity.time_column, entity.time_column_type, False),
        t.StructField(get_target_id_column_name(), t.StringType(), False),
    ]
