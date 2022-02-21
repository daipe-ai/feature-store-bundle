from datetime import datetime as dt
from logging import Logger
from typing import List, Optional

from daipecore.widgets.Widgets import Widgets
from pyspark.sql import DataFrame, functions as f

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.notebook.functions.time_windows import get_max_time_window


class TimestampAdder:
    def __init__(self, default_time_windows: List[str], logger: Logger, widgets: Widgets, feature_store: FeatureStore):
        self.__default_time_windows = default_time_windows
        self.__logger = logger
        self.__widgets = widgets
        self.__feature_store = feature_store

    def add_without_filters(self, df: DataFrame, entity: Entity):
        target_name = self.__widgets.get_value("target_name")

        return self.__add_timestamps(df, entity) if target_name == "<no target>" else self.__add_targets(target_name, df, entity)

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
        timestamp = dt.strptime(self.__widgets.get_value("timestamp"), "%Y%m%d")
        self.__logger.info(f"No target was selected, adding `{entity.time_column}` with value `{timestamp}`")

        columns = df.columns
        columns.remove(entity.id_column)
        return df.select(entity.id_column, f.lit(timestamp).alias(entity.time_column), *columns)

    def __add_targets(self, target_name: str, df: DataFrame, entity: Entity) -> DataFrame:
        time_shift = self.__widgets.get_value("number_of_time_units")
        target_date_from = dt.strptime(self.__widgets.get_value("target_date_from"), "%Y%m%d")
        target_date_to = dt.strptime(self.__widgets.get_value("target_date_to"), "%Y%m%d")
        self.__logger.info(f"Loading targets for selected target={target_name}")

        targets = self.__feature_store.get_targets(
            entity.name,
            target_name,
            target_date_from,
            target_date_to,
            time_shift,
        )

        self.__logger.info("Joining targets with the input data")
        return df.join(targets, on=[entity.id_column], how="inner")
