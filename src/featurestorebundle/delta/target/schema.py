import pyspark.sql.types as t
from featurestorebundle.entity.Entity import Entity


def get_id_column_name(entity: Entity):
    return get_entity_targets_schema(entity)[0].name


def get_time_column_name(entity: Entity):
    return get_entity_targets_schema(entity)[1].name


def get_target_id_column_name(entity: Entity):
    return get_entity_targets_schema(entity)[2].name


def get_entity_targets_schema(entity: Entity):
    return [
        t.StructField(entity.id_column, entity.id_column_type, False),
        t.StructField(entity.time_column, entity.time_column_type, False),
        t.StructField("target_id", t.StringType(), False),
    ]
