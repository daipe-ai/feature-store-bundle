from pyspark.sql import Column, functions as f
from typing import Any, List

from pyspark.sql import DataFrame

HOUR = 60 * 60
DAY = 24 * HOUR
WEEK = 7 * DAY

PERIODS = {
    "h": HOUR,
    "d": DAY,
    "w": WEEK,
}

time_window_column_template = "is_time_window_{time_window}"


def windowed(column: Column, time_window: str, default_value: Any = 0) -> Column:
    time_window_col_name = time_window_column_template.format(time_window=time_window)
    return f.when(f.col(time_window_col_name), column).otherwise(f.lit(default_value))


def with_time_windows(df: DataFrame, window_col: str, target_date_column: Column, time_windows: List) -> DataFrame:
    for time_window in time_windows:
        df = df.withColumn(
            time_window_column_template.format(time_window=time_window),
            f.col(window_col).cast("long") >= (target_date_column.cast("long") - PERIODS[time_window[-1]] * int(time_window[:-1])),
        )

    return df
