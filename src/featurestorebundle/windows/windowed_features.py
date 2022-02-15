from pyspark.sql import Column, functions as f
from typing import Any, List, Callable

from pyspark.sql import DataFrame


__HOUR = 60 * 60
__DAY = 24 * __HOUR
__WEEK = 7 * __DAY

PERIODS = {
    "h": __HOUR,
    "d": __DAY,
    "w": __WEEK,
}

# pylint: disable=invalid-name
__time_window_column_template = "is_time_window_{time_window}"


def _is_past_time_window(timestamp: Column, time_column_to_be_subtracted: Column, time_window: str) -> Column:
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


def __windowed(time_window_column_template: str, column: Column, time_window: str, default_value: Any = 0) -> Column:
    time_window_col_name = time_window_column_template.format(time_window=time_window)
    return f.when(f.col(time_window_col_name), column).otherwise(f.lit(default_value))


def with_time_windows(df: DataFrame, timestamp: str, time_column_name: str, time_windows: List[str]) -> DataFrame:
    return __with_time_windows(df, timestamp, time_column_name, time_windows, _is_past_time_window, __time_window_column_template)


def windowed(column: Column, time_window: str, default_value: Any = 0) -> Column:
    return __windowed(__time_window_column_template, column, time_window, default_value)
