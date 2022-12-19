import datetime as dt
from typing import List, Dict
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.widgets.WidgetsGetter import WidgetsGetter
from featurestorebundle.utils.DateParser import DateParser
from featurestorebundle.delta.feature.writer.DeltaFeaturesMergeConfig import DeltaFeaturesMergeConfig
from featurestorebundle.notebook.functions.time_windows import parse_time_window


class DeltaFeaturesMergeConfigGenerator:
    def __init__(self, timestamp_shift: str, widgets_getter: WidgetsGetter, date_parser: DateParser):
        self.__timestamp_shift = parse_time_window(timestamp_shift)
        self.__widgets_getter = widgets_getter
        self.__date_parser = date_parser

    def generate(
        self,
        entity: Entity,
        features_data: DataFrame,
        pk_columns: List[str],
    ) -> DeltaFeaturesMergeConfig:
        technical_cols = [entity.id_column, entity.time_column]
        data_column_names = list(
            map(
                lambda field: field.name,
                filter(lambda field: field.name not in technical_cols, features_data.schema.fields),
            )
        )

        update_set = self.__build_set(data_column_names)
        insert_set = {**update_set, **self.__build_set(technical_cols)}
        merge_condition = self.__build_merge_condition(pk_columns, entity)

        return DeltaFeaturesMergeConfig(
            source="source",
            target="target",
            data=features_data,
            merge_condition=merge_condition,
            update_set=update_set,
            insert_set=insert_set,
        )

    def __wrap_source(self, column: str) -> str:
        return self.__wrap("source", column)

    def __wrap_target(self, column: str) -> str:
        return self.__wrap("target", column)

    def __build_set(self, columns: List[str]) -> Dict[str, str]:
        return {column: self.__wrap_source(column) for column in columns}

    def __wrap(self, alias: str, column: str) -> str:  # noqa
        return f"{alias}.`{column}`"

    def __build_merge_condition(self, pk_columns: List[str], entity: Entity) -> str:
        merge_condition = " AND ".join(f"{self.__wrap_target(pk)} = {self.__wrap_source(pk)}" for pk in pk_columns)

        if self.__widgets_getter.timestamp_exists():
            # timestamp column partition pruning condition to avoid scanning whole table during delta merge
            timestamp = self.__date_parser.parse_date(self.__widgets_getter.get_timestamp()) + dt.timedelta(**self.__timestamp_shift)
            merge_condition += f" AND target.{entity.time_column} = timestamp('{timestamp}')"

        return merge_condition
