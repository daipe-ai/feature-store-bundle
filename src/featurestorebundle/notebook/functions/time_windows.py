from functools import partial

from pyspark.sql import Column, functions as f
from typing import List, Callable, Any, Union

from pyspark.sql import DataFrame


WindowedColumn = Callable[[str], Column]


__HOUR = 60 * 60
__DAY = 24 * __HOUR
__WEEK = 7 * __DAY

PERIODS = {
    "h": __HOUR,
    "d": __DAY,
    "w": __WEEK,
}

# pylint: disable=invalid-name
_time_window_column_template = "is_time_window_{time_window}"


def is_past_time_window(timestamp: Column, time_column_to_be_subtracted: Column, time_window: str) -> Column:
    period = PERIODS[time_window[-1]] * int(time_window[:-1])
    delta = timestamp - time_column_to_be_subtracted
    return (0 <= delta) & (delta <= period)


def resolve_column_type(df: DataFrame, window_col: str) -> Column:
    dtypes = dict(df.dtypes)

    if window_col not in dtypes:
        raise ValueError(f"Column {window_col} not found in dataframe.")

    dtype = dtypes[window_col]

    if dtype == "date":
        return f.to_timestamp(f.col(window_col)).cast("long")

    if dtype == "timestamp":
        return f.col(window_col).cast("long")

    raise TypeError(f"Column {window_col} is of unsupported type '{dtype}'. Must be either 'date' or 'timestamp'.")


def __with_time_windows(
    df: DataFrame,
    timestamp: str,
    time_column_name: str,
    time_windows: List[str],
    is_time_window_function: Callable,
    time_window_column_template: str,
) -> DataFrame:
    timestamp_col = resolve_column_type(df, timestamp)
    time_column_to_be_subtracted = resolve_column_type(df, time_column_name)

    time_window_columns = [
        is_time_window_function(timestamp_col, time_column_to_be_subtracted, time_window).alias(
            time_window_column_template.format(time_window=time_window)
        )
        for time_window in time_windows
    ]

    return df.select("*", *time_window_columns)


def with_time_windows(df: DataFrame, timestamp: str, time_column_name: str, time_windows: List[str]) -> DataFrame:
    return __with_time_windows(df, timestamp, time_column_name, time_windows, is_past_time_window, _time_window_column_template)


def windowed(col: Column, time_window: str, default_value: Any) -> Column:
    time_window_col_name = _time_window_column_template.format(time_window=time_window)
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
