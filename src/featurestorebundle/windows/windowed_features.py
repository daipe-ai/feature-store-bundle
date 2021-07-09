from datetime import datetime
from typing import Dict, List, Union
from pyspark.sql import Column, DataFrame, functions as f

from featurestorebundle.windows.WindowedCol import WindowedCol, windowed_col_name, get_windowed_columns
from featurestorebundle.windows.WindowedColForAgg import WindowedColForAgg


def with_windowed_columns(
    df: DataFrame,
    windowed_columns: List[WindowedCol],
    windows: List,
) -> DataFrame:
    for windowed_col in windowed_columns:
        for window in windows:
            df = df.withColumn(windowed_col.col_name(window), windowed_col.as_col(window))
    return df


def get_windowed_aggregations(
    windowed_columns: List[WindowedColForAgg], windows: List, window_col: str, target_date: Union[Column, datetime]
) -> List[Column]:
    if isinstance(target_date, datetime):
        target_date = f.lit(target_date)

    is_window = f.col(window_col).cast("long") >= (target_date.cast("long") - "({period} * {window_number})")

    return get_windowed_columns(
        [windowed_col.with_changed_col(f.when(is_window, windowed_col.col).otherwise(None)) for windowed_col in windowed_columns],
        windows,
    )


def with_windowed_columns_renamed(df: DataFrame, windows: List, columns: Dict[str, str]) -> DataFrame:
    for col_old, col_new in columns.items():
        for window in windows:
            df = df.withColumnRenamed(windowed_col_name(col_old, window), windowed_col_name(col_new, window))
    return df


def drop_windowed_columns(df: DataFrame, windows: List, *col_names) -> DataFrame:
    cols_to_drop = []
    for col_name in col_names:
        cols_to_drop.extend(get_windowed_column_names(col_name, windows))
    return df.drop(*cols_to_drop)


def get_windowed_column_names(col_name: str, windows: List) -> List[str]:
    return [windowed_col_name(col_name, window) for window in windows]
