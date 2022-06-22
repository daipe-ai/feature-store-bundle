from typing import Union, Callable
from pyspark.sql import types as t
from pyspark.sql.column import Column
from featurestorebundle.notebook.functions.time_windows import windowed_column_with_metadata, WindowedColumn
from featurestorebundle.utils.column import get_column_name

CATEGORICAL = "categorical"
NUMERICAL = "numerical"
BINARY = "binary"

variable_type_defaults = {
    "string": CATEGORICAL,
    "boolean": BINARY,
    "byte": NUMERICAL,
    "short": NUMERICAL,
    "integer": NUMERICAL,
    "long": NUMERICAL,
    "float": NUMERICAL,
    "double": NUMERICAL,
}

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


def get_variable_type_default(dtype: str) -> str:
    if dtype.startswith("decimal"):
        return NUMERICAL

    return variable_type_defaults.get(dtype)


def make_categorical(col: Union[Column, WindowedColumn]) -> Union[Column, WindowedColumn]:
    metadata = {"variable_type": CATEGORICAL}

    if isinstance(col, Callable):
        return windowed_column_with_metadata(col.args[0])(col.args[2], col.args[1], metadata=metadata)  # noqa # pyre-ignore[16]

    return col.alias(get_column_name(col), metadata=metadata)
