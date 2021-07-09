from pyspark.sql import Column, functions as f
from typing import List


def windowed_col_name(name, window):
    return name.format(window=window)


class WindowedCol:
    HOUR = "60 * 60"
    DAY = f"24 * {HOUR}"
    WEEK = f"7 * {DAY}"

    PERIODS = {
        "h": HOUR,
        "d": DAY,
        "w": WEEK,
    }

    def __init__(self, name: str, col: Column):
        self._name = name
        self.__col = col

    @property
    def col(self):
        return self.__col

    def col_name(self, window) -> str:
        return windowed_col_name(self._name, window)

    def as_col(self, window) -> Column:
        period = window[-1]
        window_number = window[:-1]
        return self._expr(self.__col._jc.toString().format(window=window, window_number=window_number, period=WindowedCol.PERIODS[period]))

    def with_changed_col(self, new_col: Column):
        return WindowedCol(self._name, new_col)

    def _expr(self, *args, **kwargs):
        return f.expr(*args, **kwargs)


def get_windowed_columns(windowed_columns: List[WindowedCol], windows: List) -> List[Column]:
    columns = []
    for windowed_column in windowed_columns:
        columns.extend([windowed_column.as_col(window) for window in windows])
    return columns
