import pyspark.sql.types as t
from featurestorebundle.entity.Entity import Entity


def get_feature_store_initial_schema(entity: Entity):
    return t.StructType(
        [
            t.StructField(entity.id_column, entity.id_column_type, False),
            t.StructField(entity.time_column, entity.time_column_type, False),
            get_rainbow_table_hash_column(),
        ]
    )


def get_rainbow_table_hash_column():
    return t.StructField("features_hash", t.StringType(), False)


def get_rainbow_table_features_column():
    return t.StructField("computed_columns", t.ArrayType(t.StringType()), False)


def get_rainbow_table_schema():
    return t.StructType(
        [
            get_rainbow_table_hash_column(),
            get_rainbow_table_features_column(),
        ]
    )
