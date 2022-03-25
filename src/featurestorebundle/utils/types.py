from pyspark.sql import types as t

names_to_dtypes = {
    "string": t.StringType(),
    "integer": t.IntegerType(),
    "long": t.LongType(),
    "short": t.ShortType(),
}

types_to_names = {dtype: name for name, dtype in names_to_dtypes.items()}
