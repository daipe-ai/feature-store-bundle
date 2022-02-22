from typing import Optional, List, Callable, Union

from daipecore.decorator.InputDecorator import InputDecorator
from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.notebook.WindowedDataFrame import WindowedDataFrame
from featurestorebundle.notebook.services.TimestampAdder import TimestampAdder


@input_decorator_function
def make_windowed(
    input_data: Union[InputDecorator, DataFrame], entity: Entity, time_column: str, custom_time_windows: Optional[List[str]] = None
) -> Callable[[ContainerInterface], WindowedDataFrame]:
    def wrapper(container: ContainerInterface) -> WindowedDataFrame:
        default_time_windows = container.get_parameters().featurestorebundle.time_windows
        time_windows = default_time_windows if custom_time_windows is None else custom_time_windows

        df = input_data.result if isinstance(input_data, InputDecorator) else input_data
        return WindowedDataFrame(df, entity, time_column, time_windows)

    return wrapper


@input_decorator_function
def with_timestamps(
    input_data: Union[InputDecorator, DataFrame], entity: Entity, comparison_col_name: str, custom_time_windows: Optional[List[str]] = None
) -> Callable[[ContainerInterface], DataFrame]:
    def wrapper(container: ContainerInterface) -> DataFrame:
        timestamp_adder: TimestampAdder = container.get(TimestampAdder)

        df = input_data.result if isinstance(input_data, InputDecorator) else input_data
        return timestamp_adder.add(df, entity, comparison_col_name, custom_time_windows)

    return wrapper


@input_decorator_function
def with_timestamps_no_filter(input_data: Union[InputDecorator, DataFrame], entity: Entity) -> Callable[[ContainerInterface], DataFrame]:
    def wrapper(container: ContainerInterface) -> DataFrame:
        timestamp_adder: TimestampAdder = container.get(TimestampAdder)

        df = input_data.result if isinstance(input_data, InputDecorator) else input_data
        return timestamp_adder.add_without_filters(df, entity)

    return wrapper
