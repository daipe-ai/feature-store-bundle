from datetime import datetime as dt, timedelta
from logging import Logger
from typing import List, Optional

from daipecore.widgets.Widgets import Widgets
from pyspark.sql import DataFrame, functions as f

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.notebook.functions.time_windows import get_max_time_window, parse_time_window
from featurestorebundle.widgets.WidgetsFactory import WidgetsFactory


class TimestampAdder:
    date_format = "%Y-%m-%d"
    legacy_date_format = "%Y%m%d"

    def __init__(
        self, timestamp_shift: str, default_time_windows: List[str], logger: Logger, widgets: Widgets, feature_store: FeatureStore
    ):
        self.__timestamp_shift = parse_time_window(timestamp_shift)
        self.__default_time_windows = default_time_windows
        self.__logger = logger
        self.__widgets = widgets
        self.__feature_store = feature_store

    def add_without_filters(self, df: DataFrame, entity: Entity):
        target_name = self.__widgets.get_value(WidgetsFactory.target_name)

        return (
            self.__add_timestamps(df, entity)
            if target_name == WidgetsFactory.no_targets_placeholder
            else self.__add_targets(target_name, df, entity)
        )

    def add(self, df: DataFrame, entity: Entity, comparison_col_name: str, custom_time_windows: Optional[List[str]]) -> DataFrame:
        time_windows = self.__default_time_windows if custom_time_windows is None else custom_time_windows

        return self.add_without_filters(df, entity).filter(
            f.col(comparison_col_name)
            .cast("timestamp")
            .between(
                (f.col(entity.time_column).cast("timestamp").cast("long") - get_max_time_window(time_windows)[1]).cast("timestamp"),
                f.col(entity.time_column),
            )
        )

    def __add_timestamps(self, df: DataFrame, entity: Entity) -> DataFrame:
        timestamp = self.__parse_date(self.__widgets.get_value(WidgetsFactory.timestamp_name)) + timedelta(**self.__timestamp_shift)
        self.__logger.info(f"No target was selected, adding `{entity.time_column}` with value `{timestamp}`")

        columns = df.columns
        columns.remove(entity.id_column)
        return df.select(entity.id_column, f.lit(timestamp).alias(entity.time_column), *columns)

    def __add_targets(self, target_name: str, df: DataFrame, entity: Entity) -> DataFrame:
        target_time_shift = self.__widgets.get_value(WidgetsFactory.target_time_shift)
        target_date_from = self.__parse_date(self.__widgets.get_value(WidgetsFactory.target_date_from_name))
        target_date_to = self.__parse_date(self.__widgets.get_value(WidgetsFactory.target_date_to_name))
        self.__logger.info(f"Loading targets for selected target={target_name}")

        targets = self.__feature_store.get_targets(
            entity.name,
            target_name,
            target_date_from,
            target_date_to,
            target_time_shift,
        )

        self.__logger.info("Joining targets with the input data")
        return df.join(targets, on=[entity.id_column], how="inner")

    def __parse_date(self, date_str: str) -> dt:
        try:
            result = dt.strptime(date_str, TimestampAdder.date_format)
        except ValueError:
            result = self.__parse_legacy_date(date_str)
        return result

    def __parse_legacy_date(self, date_str: str) -> dt:
        try:
            timestamp = dt.strptime(date_str, TimestampAdder.legacy_date_format)
            self.__logger.warning(
                f"Timestamp widget value `{date_str}` is in a deprecated format please use `{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}` instead"
            )
            return timestamp
        except ValueError as value_error:
            raise Exception(
                f"Timestamp widget value `{date_str}` does not match either `{TimestampAdder.date_format}` or `{TimestampAdder.legacy_date_format}`"
            ) from value_error
