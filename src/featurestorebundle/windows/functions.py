from functools import partial, reduce
from typing import Callable, List, Any, Union

from pyspark.sql import Column
from pyspark.sql import functions as f

WindowedColumn = Callable[[str], Column]


def windowed(col: Column, time_window: str, default_value: Any) -> Column:
    time_window_col_name = f"is_time_window_{time_window}"
    return f.when(f.col(time_window_col_name), col).otherwise(default_value)


def __windowed_col(fun: Callable, cols: List[Column], name: str, default_value: Any, time_window: str) -> Column:
    return fun(
        *(
            windowed(
                col,
                time_window,
                default_value,
            )
            for col in cols
        )
    ).alias(name)


def windowed_column(fun: Callable):
    def wrapper(name: str, cols: Union[Column, List[Column]], default_value=None):
        cols = cols if isinstance(cols, list) else [cols]
        return partial(__windowed_col, fun, cols, name, default_value)

    return wrapper


def sum_windowed(name: str, col: Column, default_value=None) -> WindowedColumn:
    return windowed_column(f.sum)(name, col, default_value)


def count_windowed(name: str, col: Column, default_value=None) -> WindowedColumn:
    return windowed_column(f.count)(name, col, default_value)


def count_distinct_windowed(name: str, cols: List[Column], default_value=None) -> WindowedColumn:
    return windowed_column(f.countDistinct)(name, cols, default_value)


def min_windowed(name: str, col: Column, default_value=None) -> WindowedColumn:
    return windowed_column(f.min)(name, col, default_value)


def max_windowed(name: str, col: Column, default_value=None) -> WindowedColumn:
    return windowed_column(f.max)(name, col, default_value)


def mean_windowed(name: str, col: Column, default_value=None) -> WindowedColumn:
    return windowed_column(f.mean)(name, col, default_value)


def avg_windowed(name: str, col: Column, default_value=None) -> WindowedColumn:
    return windowed_column(f.avg)(name, col, default_value)


def first_windowed(name: str, col: Column, default_value=None) -> WindowedColumn:
    return windowed_column(f.first)(name, col, default_value)


def array_contains_any(col: Column, values: List[str]) -> Column:
    if not values:
        return f.lit(True)

    return reduce(
        lambda first_condition, second_condition: (first_condition) | (second_condition), [f.array_contains(col, value) for value in values]
    )


def column(name: str, col: Column) -> Column:
    return col.alias(name)


def most_common(name: str, *columns: Column) -> Column:
    return f.max(f.struct(*columns)).alias(name)
