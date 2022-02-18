from logging import Logger
from datetime import datetime as dt
from typing import Optional, List, Callable, Union

from pyspark.sql import DataFrame, functions as f

from daipecore.widgets.Widgets import Widgets
from daipecore.decorator.InputDecorator import InputDecorator
from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.notebook.WindowedDataFrame import WindowedDataFrame


@input_decorator_function
def make_windowed(
    input_decorator: InputDecorator, entity: Entity, time_column: str, custom_time_windows: Optional[List[str]] = None
) -> Callable[[ContainerInterface], WindowedDataFrame]:
    def wrapper(container: ContainerInterface) -> WindowedDataFrame:
        default_time_windows = container.get_parameters().featurestorebundle.time_windows
        time_windows = default_time_windows if custom_time_windows is None else custom_time_windows

        return WindowedDataFrame(input_decorator.result, entity, time_column, time_windows)

    return wrapper


@input_decorator_function
def with_timestamps(input_data: Union[InputDecorator, DataFrame], entity: Entity) -> Callable[[ContainerInterface], DataFrame]:
    def wrapper(container: ContainerInterface) -> DataFrame:
        df = input_data.result if isinstance(input_data, InputDecorator) else input_data

        widgets: Widgets = container.get(Widgets)
        logger: Logger = container.get("featurestorebundle.logger")

        timestamp = dt.strptime(widgets.get_value("timestamp"), "%Y%m%d")
        target_name = widgets.get_value("target_name")

        if target_name == "<no target>":
            logger.info(f"No target was selected, adding `{entity.time_column}` with value `{timestamp}`")
            columns = df.columns
            columns.remove(entity.id_column)
            return df.select(entity.id_column, f.lit(timestamp).alias(entity.time_column), *columns)

        feature_store: FeatureStore = container.get(FeatureStore)

        time_shift = widgets.get_value("number_of_time_units")
        target_date_from = dt.strptime(widgets.get_value("target_date_from"), "%Y%m%d")
        target_date_to = dt.strptime(widgets.get_value("target_date_to"), "%Y%m%d")

        logger.info(f"Loading targets for selected target={target_name}")
        targets = feature_store.get_targets(
            entity.name,
            target_name,
            target_date_from,
            target_date_to,
            time_shift,
        )

        logger.info("Joining targets with the input data")
        return df.join(targets, on=[entity.id_column], how="inner")

    return wrapper
