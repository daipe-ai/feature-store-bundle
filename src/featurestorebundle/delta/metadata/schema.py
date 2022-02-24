import pyspark.sql.types as t


def get_metadata_pk_columns():
    return [
        t.StructField("entity", t.StringType(), False),
        t.StructField("feature", t.StringType(), False),
    ]


def get_metadata_columns():
    return [
        t.StructField("description", t.StringType(), True),
        t.StructField("extra", t.MapType(t.StringType(), t.StringType(), True)),
        t.StructField("feature_template", t.StringType(), True),
        t.StructField("category", t.StringType(), True),
        t.StructField("dtype", t.StringType(), True),
        t.StructField("default_value", t.StringType(), True),
    ]


def get_metadata_schema():
    return t.StructType(get_metadata_pk_columns() + get_metadata_columns())


def get_metadata_string_schema():
    return ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in get_metadata_schema()])
