from typing import Optional, List, Callable, Union

from daipecore.decorator.InputDecorator import InputDecorator
from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesGetter import FeaturesGetter
from featurestorebundle.notebook.WindowedDataFrame import WindowedDataFrame
from featurestorebundle.notebook.services.TimestampAdder import TimestampAdder


class WrongColumnTypeError(Exception):
    pass


def __check_time_column_type(df: DataFrame, time_column_name: str):
    dtypes_dict = dict(df.dtypes)

    if time_column_name not in dtypes_dict:
        raise Exception(f"Column `{time_column_name}` is not present in the input DataFrame")

    if dtypes_dict[time_column_name] not in ["date", "timestamp"]:
        raise WrongColumnTypeError(
            f"Column `{time_column_name}` is neither of DateType or of TimestampType. Please convert it or use another appropriate column"
        )


@input_decorator_function
def make_windowed(
    input_data: Union[InputDecorator, DataFrame], entity: Entity, time_column_name: str, custom_time_windows: Optional[List[str]] = None
) -> Callable[[ContainerInterface], WindowedDataFrame]:
    df = input_data.result if isinstance(input_data, InputDecorator) else input_data
    __check_time_column_type(df, time_column_name)

    def wrapper(container: ContainerInterface) -> WindowedDataFrame:
        default_time_windows = container.get_parameters().featurestorebundle.time_windows
        time_windows = default_time_windows if custom_time_windows is None else custom_time_windows

        return WindowedDataFrame(df, entity, time_column_name, time_windows)

    return wrapper


@input_decorator_function
def with_timestamps(
    input_data: Union[InputDecorator, DataFrame], entity: Entity, time_column_name: str, custom_time_windows: Optional[List[str]] = None
) -> Callable[[ContainerInterface], DataFrame]:
    df = input_data.result if isinstance(input_data, InputDecorator) else input_data
    __check_time_column_type(df, time_column_name)

    def wrapper(container: ContainerInterface) -> DataFrame:
        timestamp_adder: TimestampAdder = container.get(TimestampAdder)

        return timestamp_adder.add(df, entity, time_column_name, custom_time_windows)

    return wrapper


@input_decorator_function
def with_timestamps_no_filter(input_data: Union[InputDecorator, DataFrame], entity: Entity) -> Callable[[ContainerInterface], DataFrame]:
    def wrapper(container: ContainerInterface) -> DataFrame:
        timestamp_adder: TimestampAdder = container.get(TimestampAdder)

        df = input_data.result if isinstance(input_data, InputDecorator) else input_data
        return timestamp_adder.add_without_filters(df, entity)

    return wrapper


@input_decorator_function
def get_features() -> Callable[[ContainerInterface], DataFrame]:
    def wrapper(container: ContainerInterface) -> DataFrame:
        features_getter: FeaturesGetter = container.get(FeaturesGetter)
        return features_getter.get_features()

    return wrapper
