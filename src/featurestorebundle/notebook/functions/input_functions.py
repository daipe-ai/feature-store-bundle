from typing import Optional, List, Callable, Union

from daipecore.decorator.InputDecorator import InputDecorator
from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesGetter import FeaturesGetter
from featurestorebundle.notebook.WindowedDataFrame import WindowedDataFrame
from featurestorebundle.notebook.services.TimestampAdder import TimestampAdder
from featurestorebundle.utils.ColumnChecker import ColumnChecker
from featurestorebundle.utils.types import types_to_names


@input_decorator_function
def make_windowed(
    input_data: Union[InputDecorator, DataFrame], entity: Entity, time_column_name: str, custom_time_windows: Optional[List[str]] = None
) -> Callable[[ContainerInterface], WindowedDataFrame]:
    df = input_data.result if isinstance(input_data, InputDecorator) else input_data

    def wrapper(container: ContainerInterface) -> WindowedDataFrame:
        column_checker: ColumnChecker = container.get(ColumnChecker)
        column_checker.check_column_type(df, entity.id_column, [types_to_names[entity.id_column_type]])
        column_checker.check_column_type(df, time_column_name, ["date", "timestamp"])

        default_time_windows = container.get_parameters().featurestorebundle.time_windows
        time_windows = default_time_windows if custom_time_windows is None else custom_time_windows

        return WindowedDataFrame(df, entity, time_column_name, time_windows)

    return wrapper


@input_decorator_function
def with_timestamps(
    input_data: Union[InputDecorator, DataFrame], entity: Entity, time_column_name: str, custom_time_windows: Optional[List[str]] = None
) -> Callable[[ContainerInterface], DataFrame]:
    df = input_data.result if isinstance(input_data, InputDecorator) else input_data

    def wrapper(container: ContainerInterface) -> DataFrame:
        column_checker: ColumnChecker = container.get(ColumnChecker)
        column_checker.check_column_type(df, entity.id_column, [types_to_names[entity.id_column_type]])
        column_checker.check_column_type(df, time_column_name, ["date", "timestamp"])

        timestamp_adder: TimestampAdder = container.get(TimestampAdder)
        return timestamp_adder.add(df, entity, time_column_name, custom_time_windows)

    return wrapper


@input_decorator_function
def with_timestamps_no_filter(input_data: Union[InputDecorator, DataFrame], entity: Entity) -> Callable[[ContainerInterface], DataFrame]:
    df = input_data.result if isinstance(input_data, InputDecorator) else input_data

    def wrapper(container: ContainerInterface) -> DataFrame:
        column_checker: ColumnChecker = container.get(ColumnChecker)
        column_checker.check_column_type(df, entity.id_column, [types_to_names[entity.id_column_type]])

        timestamp_adder: TimestampAdder = container.get(TimestampAdder)
        return timestamp_adder.add_without_filters(df, entity)

    return wrapper


@input_decorator_function
def get_features() -> Callable[[ContainerInterface], DataFrame]:
    def wrapper(container: ContainerInterface) -> DataFrame:
        features_getter: FeaturesGetter = container.get(FeaturesGetter)
        return features_getter.get_features()

    return wrapper
