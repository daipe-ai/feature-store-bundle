from functools import partial
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import functions as f

WindowedColumn = Callable[[str], Column]


def windowed(col: Column, time_window: str) -> Column:
    time_window_col_name = f"is_time_window_{time_window}"
    return f.when(f.col(time_window_col_name), col)


def __windowed_col_impl(fun: Callable, col: Column, name: str, time_window: str) -> Column:
    return fun(
        windowed(
            col,
            time_window,
        )
    ).alias(name)


def windowed_col(fun: Callable, col: Column, name: str) -> WindowedColumn:
    return partial(__windowed_col_impl, fun, col, name)


def sum_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def count_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def count_distinct_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def min_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def max_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def mean_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def avg_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def first_windowed(name: str, col: Column) -> WindowedColumn:
    return windowed_col(f.sum, col, name)


def column(name: str, col: Column) -> Column:
    return col.alias(name)
