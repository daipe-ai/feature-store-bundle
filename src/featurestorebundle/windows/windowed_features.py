from pyspark.sql import Column, functions as f
from typing import Any, List, Callable

from pyspark.sql import DataFrame


__HOUR = 60 * 60
__DAY = 24 * __HOUR
__WEEK = 7 * __DAY

__PERIODS = {
    "h": __HOUR,
    "d": __DAY,
    "w": __WEEK,
}

# pylint: disable=invalid-name
__time_window_column_template = "is_time_window_{time_window}"


def _is_past_time_window(target_date: Column, window_argument_date: Column, time_window) -> Column:
    period = __PERIODS[time_window[-1]] * int(time_window[:-1])
    delta = target_date - window_argument_date
    return (0 <= delta) & (delta <= period)


def __resolve_column_type(df: DataFrame, window_col: str) -> Column:
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
    window_col: str,
    target_date_column: Column,
    time_windows: List,
    is_time_window_function: Callable,
    time_window_column_template: str,
) -> DataFrame:
    window_argument_date = __resolve_column_type(df, window_col)
    target_date = target_date_column.cast("long")

    time_window_columns = [
        is_time_window_function(target_date, window_argument_date, time_window).alias(
            time_window_column_template.format(time_window=time_window)
        )
        for time_window in time_windows
    ]

    return df.select("*", *time_window_columns)


def __windowed(time_window_column_template: str, column: Column, time_window: str, default_value: Any = 0) -> Column:
    time_window_col_name = time_window_column_template.format(time_window=time_window)
    return f.when(f.col(time_window_col_name), column).otherwise(f.lit(default_value))


def with_time_windows(df: DataFrame, window_col: str, target_date_column: Column, time_windows: List) -> DataFrame:
    return __with_time_windows(df, window_col, target_date_column, time_windows, _is_past_time_window, __time_window_column_template)


def windowed(column: Column, time_window: str, default_value: Any = 0) -> Column:
    return __windowed(__time_window_column_template, column, time_window, default_value)
