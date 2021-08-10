from typing import Dict, List
from pyspark.sql import Column, DataFrame, functions as f

from featurestorebundle.windows.WindowedCol import WindowedCol

HOUR = 60 * 60
DAY = 24 * HOUR
WEEK = 7 * DAY

PERIODS = {
    "h": HOUR,
    "d": DAY,
    "w": WEEK,
}


def get_aggregations(windowed_columns: List[WindowedCol], windows: List, is_windows: Dict[str, Column]) -> List[Column]:
    columns = []
    for windowed_column in windowed_columns:
        columns.extend([windowed_column.to_agg_windowed_column(is_windows[window], window) for window in windows])
    return columns


def get_windowed_aggregations(windowed_columns: List[WindowedCol], windows: List, window_col: str, target_date: Column) -> List[Column]:
    is_windows = {
        window: f.col(window_col).cast("long") >= (target_date.cast("long") - PERIODS[window[-1]] * int(window[:-1])) for window in windows
    }

    return get_aggregations(windowed_columns, windows, is_windows)


def get_windowed_mapping_for_renaming(windows: List, mapping: Dict[str, str]) -> Dict[str, str]:
    windowed_mapping: Dict[str, str] = {}
    for col_old, col_new in mapping.items():
        windowed_mapping = {**windowed_mapping, **{col_old.format(window=window): col_new.format(window=window) for window in windows}}
    return windowed_mapping


def get_windowed_columns_renamed(df: DataFrame, windows: List, mapping: Dict[str, str]) -> List[Column]:
    windowed_mapping = get_windowed_mapping_for_renaming(windows, mapping)
    return [
        f.col(col_name).alias(windowed_mapping[col_name]) if col_name in windowed_mapping else f.col(col_name) for col_name in df.columns
    ]


def get_windowed_columns_to_drop(windows: List, *col_names: str) -> List[str]:
    col_names_to_drop = []
    for col_name in col_names:
        col_names_to_drop.extend(get_windowed_column_names(windows, col_name))
    return col_names_to_drop


def get_windowed_column_names(windows: List, col_name: str) -> List[str]:
    return [col_name.format(window=window) for window in windows]


def with_renamed_windowed_columns(df: DataFrame, windows: List, mapping: Dict[str, str]) -> DataFrame:
    return df.select(get_windowed_columns_renamed(df, windows, mapping))
