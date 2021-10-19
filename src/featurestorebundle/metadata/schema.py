import pyspark.sql.types as t


METADATA_PK = t.StructField("feature", t.StringType(), True)
METADATA_COLUMNS = [
    t.StructField("description", t.StringType(), True),
    t.StructField("extra", t.MapType(t.StringType(), t.StringType(), True)),
    t.StructField("feature_template", t.StringType(), True),
    t.StructField("category", t.StringType(), True),
    t.StructField("dtype", t.StringType(), True),
]
METADATA_SCHEMA = t.StructType([METADATA_PK] + METADATA_COLUMNS)
