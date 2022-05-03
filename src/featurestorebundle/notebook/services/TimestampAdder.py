from datetime import timedelta
from logging import Logger
from typing import List, Optional

from box import Box
from pyspark.sql import DataFrame, functions as f

from featurestorebundle.utils.DateParser import DateParser
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.notebook.functions.time_windows import get_max_time_window, parse_time_window
from featurestorebundle.widgets.WidgetsFactory import WidgetsFactory
from featurestorebundle.widgets.WidgetsGetter import WidgetsGetter


class TimestampAdder:
    def __init__(
        self,
        timestamp_shift: str,
        default_time_windows: List[str],
        logger: Logger,
        sampling: Box,
        widgets_getter: WidgetsGetter,
        feature_store: FeatureStore,
        date_parser: DateParser,
    ):
        self.__timestamp_shift = parse_time_window(timestamp_shift)
        self.__default_time_windows = default_time_windows
        self.__logger = logger
        self.__sampling = sampling
        self.__widgets_getter = widgets_getter
        self.__feature_store = feature_store
        self.__date_parser = date_parser

    def add_without_filters(self, df: DataFrame, entity: Entity):
        target_name = self.__widgets_getter.get_target()

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
        timestamp = self.__date_parser.parse_date(self.__widgets_getter.get_timestamp()) + timedelta(**self.__timestamp_shift)
        self.__logger.info(f"No target was selected, adding `{entity.time_column}` with value `{timestamp}`")

        if self.__widgets_getter.should_sample():
            df = df.sample(withReplacement=False, fraction=self.__sampling.rate, seed=self.__sampling.seed)
            self.__logger.info(f"Using sampling rate of {self.__sampling.rate}")

        columns = df.columns
        columns.remove(entity.id_column)
        return df.select(entity.id_column, f.lit(timestamp).alias(entity.time_column), *columns)

    def __add_targets(self, target_name: str, df: DataFrame, entity: Entity) -> DataFrame:
        target_time_shift = self.__widgets_getter.get_target_time_shift()
        target_date_from = self.__date_parser.parse_date(self.__widgets_getter.get_target_date_from())
        target_date_to = self.__date_parser.parse_date(self.__widgets_getter.get_target_date_to())
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
