from pyspark.sql import types as t

CATEGORICAL = "categorical"
NUMERICAL = "numerical"
BINARY = "binary"

names_to_dtypes = {
    "string": t.StringType(),
    "boolean": t.BooleanType(),
    "byte": t.ByteType(),
    "short": t.ShortType(),
    "integer": t.IntegerType(),
    "long": t.LongType(),
    "float": t.FloatType(),
    "double": t.DoubleType(),
}

types_to_names = {dtype: name for name, dtype in names_to_dtypes.items()}
