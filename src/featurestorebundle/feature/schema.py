import pyspark.sql.types as t
from featurestorebundle.entity.Entity import Entity


def get_feature_store_initial_schema(entity: Entity):
    return t.StructType(
        [
            t.StructField(entity.id_column, entity.id_column_type, False),
            t.StructField(entity.time_column, entity.time_column_type, False),
        ]
    )
